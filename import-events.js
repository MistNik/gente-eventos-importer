import admin from 'firebase-admin';

// =============================================================================
// v2 (sept 2026) — reescritura completa sobre import-events.legacy.js tras un
// análisis conjunto que detectó 11 puntos débiles. Resumen de qué cambió y por
// qué (cada punto está además comentado en el lugar donde se resuelve):
//   1. Batches de Firestore ahora se dividen en tandas de <=450 escrituras
//      (el límite real es 500) — ver commitInChunks().
//   2. fetchAllEvents() reintenta con backoff exponencial y, si se agotan los
//      reintentos, ABORTA todo el run (no comitea nada) en vez de reportar
//      éxito con datos incompletos — ver fetchWithRetry().
//   3. La credencial de Firebase se valida explícitamente antes de parsear.
//   4/5. matchKnownVenue() sigue siendo el primer intento (rápido, sin
//      llamadas externas), pero ahora hay un segundo paso de geocoding real
//      (Nominatim/OpenStreetMap, gratis) con caché en Firestore para todo lo
//      que no matchea la lista fija — necesario porque este sitio cubre toda
//      la provincia, no solo los ~5 lugares de Córdoba capital que estaban
//      hardcodeados. También se corrigió el split de oraciones, que rompía
//      horarios con punto decimal (ej. "17.30hs").
//   6. matchInterests() usa regex con límites de palabra (\b) en vez de
//      includes(), para evitar falsos positivos.
//   7. MAX_PAGES como tope de seguridad, independiente de total_pages.
//   8. Al final del run se borran (hard delete, decisión explícita) los
//      documentos cordobaturismo-* que ya no aparecen en la fuente — ver
//      findStaleDocs().
//   9. El email de contacto real va en el User-Agent (lo exige tanto buena
//      práctica como la política de uso de Nominatim).
//  10. Cada evento se procesa en su propio try/catch: uno con datos raros no
//      aborta el resto del batch.
//  11. El workflow de GitHub Actions ahora tiene `concurrency` para que un
//      disparo manual no se pise con la corrida programada.
// =============================================================================

// --- Config general ---
const CONTACT_EMAIL = 'trice.faud@gmail.com';
const USER_AGENT = `GenteQueConoceGente-EventsImporter/2.0 (contacto: ${CONTACT_EMAIL})`;
const SOURCE_PREFIX = 'cordobaturismo-';

const EVENTS_API_BASE = 'https://cordobaturismo.gov.ar/wp-json/tribe/events/v1/events';
const PER_PAGE = 50;
const MAX_PAGES = 60; // (7) tope de seguridad aunque total_pages diga otra cosa
const FETCH_MAX_RETRIES = 3;
const FETCH_RETRY_BASE_MS = 1000; // 1s, 2s, 4s
const FETCH_PAGE_DELAY_MS = 500; // pausa breve entre páginas, buena práctica

const FIRESTORE_BATCH_LIMIT = 450; // (1) por debajo del límite real de 500

// Nominatim: gratis, sin API key, pero exige identificarse y cachear
// resultados — ver wikitech/operations.osmfoundation.org/policies/nominatim.
const NOMINATIM_URL = 'https://nominatim.openstreetmap.org/search';
const NOMINATIM_DELAY_MS = 1100; // >1req/seg exigido por su política de uso
const GEOCODE_CACHE_COLLECTION = 'geocodeCache';

// --- Inicialización de Firebase Admin ---
// (3) Antes: JSON.parse directo sobre la env var, sin validar — un secret
// faltante o mal copiado daba un SyntaxError genérico y confuso en los logs
// de la Action. Ahora se valida explícitamente con un mensaje claro.
function initFirestore() {
    if (!process.env.FIREBASE_SERVICE_ACCOUNT) {
        console.error('Falta la variable de entorno FIREBASE_SERVICE_ACCOUNT (JSON de la cuenta de servicio de Firebase).');
        process.exit(1);
    }
    let serviceAccount;
    try {
        serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
    } catch (err) {
        console.error(`FIREBASE_SERVICE_ACCOUNT no contiene un JSON válido: ${err.message}`);
        process.exit(1);
    }
    admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
    return admin.firestore();
}
const db = initFirestore();

// --- Lugares conocidos con coordenadas (fast-path, sin llamadas externas) ---
const KNOWN_VENUES = [
    { name: 'Teatro del Libertador', lat: -31.4187, lng: -64.1877 },
    { name: 'Teatro Real', lat: -31.4155, lng: -64.1888 },
    { name: 'Quality Espacio', lat: -31.4004, lng: -64.2325 },
    { name: 'Plaza de la Música', lat: -31.4162, lng: -64.1836 },
    { name: 'Centro Cultural Córdoba', lat: -31.4187, lng: -64.1875 },
];

// Misma lista de intereses que usa el onboarding (página 3)
const ALL_INTERESTS = ['animales', 'arte', 'café', 'cocina', 'deportes', 'invierno', 'juegos de mesa', 'libros', 'museos', 'musica', 'naturaleza', 'shopping', 'teatro'];

function normalize(str) {
    return str.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
}

function escapeRegex(str) {
    return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// (6) Antes: normText.includes(normalize(interest)) — "arte"/"café" podían
// matchear como substring dentro de palabras no relacionadas. Ahora se usa
// una regex con límites de palabra (\b) por cada interés, precomputada una
// sola vez al cargar el módulo.
const INTEREST_MATCHERS = ALL_INTERESTS.map((interest) => ({
    key: interest,
    regex: new RegExp(`\\b${escapeRegex(normalize(interest))}\\b`, 'i'),
}));

function matchInterests(text) {
    const normText = normalize(text);
    return INTEREST_MATCHERS.filter(({ regex }) => regex.test(normText)).map(({ key }) => key);
}

function stripHtml(html) {
    return html
        .replace(/<[^>]*>/g, ' ')
        .replace(/&nbsp;/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function matchKnownVenue(plainText) {
    const lower = plainText.toLowerCase();
    return KNOWN_VENUES.find((v) => lower.includes(v.name.toLowerCase())) || null;
}

// (5) Antes: plainText.split('.') cortaba también en horarios con punto
// decimal (ej. "17.30hs" -> "17" / "30hs"), fragmentando la oración real.
// Ahora solo se corta en un "." seguido de espacio+mayúscula o fin de texto
// — nunca si el siguiente carácter es un dígito.
function splitSentences(plainText) {
    return plainText
        .split(/\.(?=\s+[A-ZÁÉÍÓÚÑ¡¿]|\s*$)/)
        .map((s) => s.trim())
        .filter(Boolean);
}

// Busca la última oración que menciona "Córdoba" (suele ser la de fecha/lugar)
function extractLocationText(plainText) {
    const sentences = splitSentences(plainText);
    const candidate = [...sentences].reverse().find((s) => /c[oó]rdoba/i.test(s));
    return candidate || '';
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

// (2) Reintentos con backoff exponencial. Se usa tanto para la API de
// eventos como para Nominatim. Si se agotan los reintentos, propaga el
// error — quien llame decide si eso aborta todo el run o solo se degrada
// (ver fetchAllEvents vs geocodeLocationText).
async function fetchWithRetry(url, options, maxRetries = FETCH_MAX_RETRIES) {
    let lastError;
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
        try {
            const res = await fetch(url, options);
            if (res.ok) return res;
            lastError = new Error(`HTTP ${res.status} en ${url}`);
        } catch (err) {
            lastError = err;
        }
        if (attempt < maxRetries) {
            const backoff = FETCH_RETRY_BASE_MS * 2 ** attempt;
            console.warn(`  Intento ${attempt + 1}/${maxRetries + 1} falló (${lastError.message}); reintentando en ${backoff}ms...`);
            await sleep(backoff);
        }
    }
    throw lastError;
}

async function fetchAllEvents() {
    const events = [];
    let page = 1;
    let totalPages = 1;

    do {
        const url = `${EVENTS_API_BASE}?per_page=${PER_PAGE}&page=${page}`;
        // (2) Si esto agota los reintentos, se propaga hacia main() y aborta
        // todo el run (no se comitea nada) en vez de guardar una
        // importación parcial como si hubiera sido exitosa.
        const res = await fetchWithRetry(url, { headers: { 'User-Agent': USER_AGENT } });

        const data = await res.json();
        events.push(...data.events);
        totalPages = data.total_pages;
        page++;

        if (page > MAX_PAGES) {
            console.warn(`Alcanzado el tope de seguridad de ${MAX_PAGES} páginas; se detiene la paginación aunque la API reporte más.`);
            break;
        }

        await sleep(FETCH_PAGE_DELAY_MS);
    } while (page <= totalPages);

    return events;
}

// --- Geocoding (Nominatim + caché en Firestore) ---
// (4/5) Reemplaza la dependencia exclusiva de KNOWN_VENUES (insuficiente:
// esta fuente cubre toda la provincia, no solo Córdoba capital). El caché es
// obligatorio según la política de uso de Nominatim, y además evita
// re-geocodificar cada día los mismos lugares que se repiten.
function geocodeCacheDocId(queryText) {
    // Los IDs de documento de Firestore no admiten "/"; se recorta por si
    // el texto es muy largo.
    return normalize(queryText).replace(/[/\s]+/g, '-').slice(0, 200) || 'vacio';
}

async function geocodeLocationText(locationText) {
    if (!locationText) return null;

    const docId = geocodeCacheDocId(locationText);
    const cacheRef = db.collection(GEOCODE_CACHE_COLLECTION).doc(docId);
    const cached = await cacheRef.get();
    if (cached.exists) {
        const data = cached.data();
        return data.found ? { lat: data.lat, lng: data.lng } : null;
    }

    // Cache miss: llamada real a Nominatim. Solo este camino respeta el
    // delay de NOMINATIM_DELAY_MS (>1 req/seg exigido por su política).
    const query = `${locationText}, Córdoba, Argentina`;
    const url = `${NOMINATIM_URL}?format=jsonv2&limit=1&countrycodes=ar&q=${encodeURIComponent(query)}`;
    let result = null;
    try {
        // Menos reintentos que la API de eventos: si Nominatim falla, ese
        // evento se queda sin coordenadas, pero no vale la pena insistir
        // mucho ni abortar el run por esto.
        const res = await fetchWithRetry(url, { headers: { 'User-Agent': USER_AGENT } }, 2);
        const results = await res.json();
        if (Array.isArray(results) && results.length > 0) {
            result = { lat: parseFloat(results[0].lat), lng: parseFloat(results[0].lon) };
        }
    } catch (err) {
        console.warn(`  Geocoding falló para "${locationText}": ${err.message}`);
    }

    // Se cachea también el "no encontrado" (found: false) para no volver a
    // preguntarle a Nominatim por un texto que ya sabemos que no resuelve.
    await cacheRef.set({
        query: locationText,
        found: !!result,
        lat: result ? result.lat : null,
        lng: result ? result.lng : null,
        resolvedAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    await sleep(NOMINATIM_DELAY_MS);
    return result;
}

// --- Firestore: commit en tandas ---
// (1) db.batch() tiene un límite real de 500 operaciones; se agrupa en
// tandas de FIRESTORE_BATCH_LIMIT para no depender de que la cantidad de
// eventos + bajas se mantenga siempre por debajo de eso.
async function commitInChunks(operations) {
    for (let i = 0; i < operations.length; i += FIRESTORE_BATCH_LIMIT) {
        const chunk = operations.slice(i, i + FIRESTORE_BATCH_LIMIT);
        const batch = db.batch();
        for (const op of chunk) {
            if (op.type === 'set') batch.set(op.ref, op.data, { merge: true });
            else if (op.type === 'delete') batch.delete(op.ref);
        }
        await batch.commit();
    }
}

// --- Limpieza de eventos que ya no están en la fuente ---
// (8) Antes: el importador solo hacía upsert, nunca borraba — los eventos
// vencidos/cancelados quedaban para siempre en Firestore. Ahora se borran
// (decisión explícita del usuario) los documentos cordobaturismo-* que no
// aparecieron en la corrida actual. El filtro por prefijo usa el truco
// estándar de Firestore de acotar un rango sobre documentId() (no hace falta
// ningún campo extra, y funciona retroactivamente sobre los docs ya
// existentes). Solo toca documentos con ese prefijo — nunca eventos
// cargados por otra vía.
async function findStaleDocs(currentIds) {
    const snapshot = await db.collection('events')
        .where(admin.firestore.FieldPath.documentId(), '>=', SOURCE_PREFIX)
        .where(admin.firestore.FieldPath.documentId(), '<', `${SOURCE_PREFIX}`)
        .get();

    return snapshot.docs
        .filter((doc) => !currentIds.has(doc.id))
        .map((doc) => doc.ref);
}

async function main() {
    console.log('Buscando eventos...');
    const rawEvents = await fetchAllEvents();
    console.log(`Encontrados ${rawEvents.length} eventos.`);

    const operations = [];
    const currentIds = new Set();
    let okCount = 0;
    let skippedCount = 0;
    let geocodedCount = 0;

    for (const ev of rawEvents) {
        // (10) Antes: un evento con forma inesperada podía tirar toda la
        // corrida abajo a mitad del batch.set(). Ahora cada evento se
        // procesa en su propio try/catch: si falla, se loggea y se
        // saltea, sin afectar al resto.
        try {
            const docId = `${SOURCE_PREFIX}${ev.id}`;
            currentIds.add(docId);

            const plainText = stripHtml(ev.description || '');
            const knownVenue = matchKnownVenue(plainText);
            const locationText = extractLocationText(plainText);

            let venueName = knownVenue ? knownVenue.name : null;
            let lat = knownVenue ? knownVenue.lat : null;
            let lng = knownVenue ? knownVenue.lng : null;

            if (!knownVenue && locationText) {
                const geocoded = await geocodeLocationText(locationText);
                if (geocoded) {
                    lat = geocoded.lat;
                    lng = geocoded.lng;
                    geocodedCount++;
                }
            }

            const docData = {
                title: ev.title,
                startDate: ev.start_date || null,
                endDate: ev.end_date || null,
                cost: ev.cost || '',
                imageUrl: ev.image?.url || null,
                locationText,
                venueName,
                lat,
                lng,
                sourceUrl: ev.url,
                interests: matchInterests(`${ev.title} ${plainText}`),
                updatedAt: admin.firestore.FieldValue.serverTimestamp(),
            };

            operations.push({ type: 'set', ref: db.collection('events').doc(docId), data: docData });
            okCount++;
        } catch (err) {
            skippedCount++;
            console.error(`  Evento salteado (id=${ev && ev.id}, "${ev && ev.title}"): ${err.message}`);
        }
    }

    console.log('Buscando eventos obsoletos para eliminar...');
    const staleRefs = await findStaleDocs(currentIds);
    for (const ref of staleRefs) {
        operations.push({ type: 'delete', ref });
    }

    await commitInChunks(operations);

    console.log(`Listo. ${okCount} eventos guardados/actualizados, ${skippedCount} salteados por error, ${geocodedCount} geocodificados vía Nominatim, ${staleRefs.length} eliminados por estar obsoletos.`);
}

main().catch((err) => {
    console.error('Error en la importación:', err);
    process.exit(1);
});

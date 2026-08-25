import admin from 'firebase-admin';

// --- Inicialización de Firebase Admin ---
// La credencial llega como texto plano vía variable de entorno (ver paso 6)
const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
});
const db = admin.firestore();

// --- Fase 2 (a expandir juntos): lugares conocidos con coordenadas ---
const KNOWN_VENUES = [
    { name: 'Teatro del Libertador', lat: -31.4187, lng: -64.1877 },
    { name: 'Teatro Real', lat: -31.4155, lng: -64.1888 },
    { name: 'Quality Espacio', lat: -31.4004, lng: -64.2325 },
    { name: 'Plaza de la Música', lat: -31.4162, lng: -64.1836 },
    { name: 'Centro Cultural Córdoba', lat: -31.4187, lng: -64.1875 },
];

function stripHtml(html) {
    return html
        .replace(/<[^>]*>/g, ' ')
        .replace(/&nbsp;/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function matchKnownVenue(plainText) {
    const lower = plainText.toLowerCase();
    return KNOWN_VENUES.find(v => lower.includes(v.name.toLowerCase())) || null;
}

// Busca la última oración que menciona "Córdoba" (suele ser la de fecha/lugar)
function extractLocationText(plainText) {
    const sentences = plainText.split('.').map(s => s.trim()).filter(Boolean);
    const candidate = [...sentences].reverse().find(s => /c[oó]rdoba/i.test(s));
    return candidate || '';
}

async function fetchAllEvents() {
    const events = [];
    let page = 1;
    let totalPages = 1;

    do {
        const url = `https://cordobaturismo.gov.ar/wp-json/tribe/events/v1/events?per_page=50&page=${page}`;
        const res = await fetch(url, {
            headers: { 'User-Agent': 'GenteQueConoceGente-EventsImporter/1.0 (contacto: TU_EMAIL_ACA)' },
        });

        if (!res.ok) {
            console.error(`Error en la página ${page}: ${res.status}`);
            break;
        }

        const data = await res.json();
        events.push(...data.events);
        totalPages = data.total_pages;
        page++;

        // Pausa breve entre pedidos, buena práctica aunque no sea obligatoria
        await new Promise(r => setTimeout(r, 500));
    } while (page <= totalPages);

    return events;
}

async function main() {
    console.log('Buscando eventos...');
    const rawEvents = await fetchAllEvents();
    console.log(`Encontrados ${rawEvents.length} eventos.`);

    const batch = db.batch();
    let count = 0;

    for (const ev of rawEvents) {
        const plainText = stripHtml(ev.description || '');
        const venue = matchKnownVenue(plainText);
        const locationText = extractLocationText(plainText);

        const docData = {
            title: ev.title,
            startDate: ev.start_date || null,
            endDate: ev.end_date || null,
            cost: ev.cost || '',
            imageUrl: ev.image?.url || null,
            locationText: locationText,
            venueName: venue ? venue.name : null,
            lat: venue ? venue.lat : null,
            lng: venue ? venue.lng : null,
            sourceUrl: ev.url,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        };

        const docId = `cordobaturismo-${ev.id}`;
        const docRef = db.collection('events').doc(docId);
        batch.set(docRef, docData, { merge: true });
        count++;
    }

    await batch.commit();
    console.log(`Listo. ${count} eventos guardados/actualizados en Firestore.`);
}

main().catch(err => {
    console.error('Error en la importación:', err);
    process.exit(1);
});
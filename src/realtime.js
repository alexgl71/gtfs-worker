// Aggiorna daily_trips su Atlas con i delay GTFS-RT ogni 30 secondi.
// Attivo solo dalle 07:00 alle 23:00 UTC+2.
// Env vars richieste: REMOTE_URI + REALTIME_URL_<CITTÀ> per ogni città attiva.
// Es: REALTIME_URL_TORINO, REALTIME_URL_FIRENZE

require('dotenv').config();
const { MongoClient } = require('mongodb');
const axios = require('axios');
const GtfsRealtime = require('gtfs-realtime-bindings');
const FeedMessage = GtfsRealtime.transit_realtime.FeedMessage;
const { log } = require('./logger');

const MONGO_URI = process.env.REMOTE_URI;
if (!MONGO_URI) { console.error('REMOTE_URI non impostato'); process.exit(1); }

const CITIES = ['Torino', 'Roma', 'Firenze']
  .map(name => ({ name, url: process.env[`REALTIME_URL_${name.toUpperCase()}`] }))
  .filter(c => c.url);

if (CITIES.length === 0) {
  console.error('Nessuna REALTIME_URL_<CITTÀ> configurata');
  process.exit(1);
}

function isOperatingHours() {
  const hourUTC2 = (new Date().getUTCHours() + 2) % 24;
  return hourUTC2 >= 7 && hourUTC2 < 23;
}

async function fetchRealtime(db, cityName, url) {
  const t = performance.now();
  try {
    const response = await axios.get(url, {
      responseType: 'arraybuffer',
      timeout: 30000,
      headers: { 'User-Agent': 'gtfs-worker/1.0' },
    });

    const feed = FeedMessage.decode(new Uint8Array(response.data));
    const updates = [];

    for (const entity of feed.entity) {
      const tripUpdate = entity.tripUpdate;
      if (!tripUpdate?.trip?.tripId || !tripUpdate.stopTimeUpdate?.length) continue;
      const best = [...tripUpdate.stopTimeUpdate]
        .filter(stu => stu.stopSequence != null && (stu.arrival?.delay != null || stu.departure?.delay != null))
        .sort((a, b) => b.stopSequence - a.stopSequence)[0];
      if (!best) continue;
      updates.push({
        trip_id:   tripUpdate.trip.tripId.trim(),
        delay_sec: best.arrival?.delay ?? best.departure?.delay,
      });
    }

    const trips = db.collection('daily_trips');
    await trips.updateMany({ delay_sec: { $exists: true } }, { $unset: { delay_sec: '' } });

    let modifiedCount = 0;
    if (updates.length > 0) {
      const result = await trips.bulkWrite(
        updates.map(u => ({
          updateOne: { filter: { trip_id: u.trip_id }, update: { $set: { delay_sec: u.delay_sec } } },
        })),
        { ordered: false }
      );
      modifiedCount = result.modifiedCount;
    }

    await log('realtime_update', {
      city: cityName,
      modified_count: modifiedCount,
      entities: feed.entity.length,
      duration_ms: Math.round(performance.now() - t),
    });

  } catch (err) {
    await log('realtime_error', {
      city: cityName,
      error: err.message,
      duration_ms: Math.round(performance.now() - t),
    });
  }
}

async function main() {
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  await log('realtime_start', { cities: CITIES.map(c => c.name) });

  const dbs = Object.fromEntries(CITIES.map(c => [c.name, client.db(c.name)]));

  async function runAll() {
    await Promise.allSettled(CITIES.map(c => fetchRealtime(dbs[c.name], c.name, c.url)));
  }

  if (isOperatingHours()) await runAll();
  setInterval(() => { if (isOperatingHours()) runAll(); }, 30000);
}

main().catch(async (err) => {
  await log('realtime_fatal', { error: err.message });
  process.exit(1);
});

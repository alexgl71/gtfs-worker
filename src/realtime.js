// Aggiorna daily_trips su Atlas con delay e posizioni GTFS-RT ogni 30 secondi.
// Aggiorna collection alerts per ogni città ogni 10 minuti.
// Attivo solo dalle 07:00 alle 23:00 UTC+2.
// Env vars: REMOTE_URI + REALTIME_URL_* + VEHICLES_URL_* + ALERTS_URL_*

require('dotenv').config();
const { MongoClient } = require('mongodb');
const axios = require('axios');
const GtfsRealtime = require('gtfs-realtime-bindings');
const FeedMessage = GtfsRealtime.transit_realtime.FeedMessage;
const { log, logError } = require('./logger');

const MONGO_URI = process.env.REMOTE_URI;
if (!MONGO_URI) { console.error('REMOTE_URI non impostato'); process.exit(1); }

function cityName(key, prefix) {
  const raw = key.replace(prefix, '');
  return raw.charAt(0) + raw.slice(1).toLowerCase();
}

const CITIES = Object.entries(process.env)
  .filter(([key]) => key.startsWith('REALTIME_URL_'))
  .map(([key, url]) => ({ name: cityName(key, 'REALTIME_URL_'), url }));

const VEHICLE_CITIES = Object.entries(process.env)
  .filter(([key]) => key.startsWith('VEHICLES_URL_'))
  .map(([key, url]) => ({ name: cityName(key, 'VEHICLES_URL_'), url }));

const ALERT_CITIES = Object.entries(process.env)
  .filter(([key]) => key.startsWith('ALERTS_URL_'))
  .map(([key, url]) => ({ name: cityName(key, 'ALERTS_URL_'), url }));

if (CITIES.length === 0 && VEHICLE_CITIES.length === 0 && ALERT_CITIES.length === 0) {
  console.error('Nessuna REALTIME_URL_*, VEHICLES_URL_* o ALERTS_URL_* configurata');
  process.exit(1);
}

function isOperatingHours() {
  const hourUTC2 = (new Date().getUTCHours() + 2) % 24;
  return hourUTC2 >= 7 && hourUTC2 < 23;
}

// Città che espongono il feed TripUpdates in JSON invece che protobuf
const JSON_REALTIME_CITIES = new Set(['Bari']);
// Città che espongono il feed VehiclePosition in JSON invece che protobuf
const JSON_VEHICLES_CITIES = new Set(['Bari']);
// Città che espongono il feed Alerts in JSON invece che protobuf
const JSON_ALERTS_CITIES = new Set(['Bari']);


function parseUpdates(cityName, response) {
  if (JSON_REALTIME_CITIES.has(cityName)) {
    const entities = response.data?.Entities ?? [];
    return {
      count: entities.length,
      updates: entities
        .filter(e => e.TripUpdate?.Trip?.TripId && e.TripUpdate.Delay != null)
        .map(e => ({ trip_id: e.TripUpdate.Trip.TripId.trim(), delay_sec: e.TripUpdate.Delay })),
    };
  }

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
  return { count: feed.entity.length, updates };
}

async function fetchRealtime(db, cityName, url) {
  const t = performance.now();
  try {
    const isJson = JSON_REALTIME_CITIES.has(cityName);
    const response = await axios.get(url, {
      responseType: isJson ? 'json' : 'arraybuffer',
      timeout: 30000,
      headers: { 'User-Agent': 'gtfs-worker/1.0' },
    });

    const { count, updates } = parseUpdates(cityName, response);

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
      entities: count,
      duration_ms: Math.round(performance.now() - t),
      Note: `Aggiornamento realtime per ${cityName}: ${modifiedCount} trip aggiornati su ${count} entità`,
    });

  } catch (err) {
    await logError('realtime_error', {
      city: cityName,
      error: err.message,
      duration_ms: Math.round(performance.now() - t),
      Note: `Errore realtime per ${cityName}: ${err.message}`,
    });
  }
}

function parseVehicleUpdates(cityName, response) {
  if (JSON_VEHICLES_CITIES.has(cityName)) {
    const entities = response.data?.Entities ?? [];
    return {
      count: entities.length,
      updates: entities
        .filter(e => e.Vehicle?.Trip?.TripId && e.Vehicle.Position?.Latitude != null)
        .map(e => ({
          trip_id:  e.Vehicle.Trip.TripId.trim(),
          position: { lat: e.Vehicle.Position.Latitude, lng: e.Vehicle.Position.Longitude },
          bearing:  null,
        })),
    };
  }

  const feed = FeedMessage.decode(new Uint8Array(response.data));
  const updates = [];
  for (const entity of feed.entity) {
    const vp = entity.vehicle;
    if (!vp?.trip?.tripId || !vp.position) continue;
    const { latitude, longitude, bearing } = vp.position;
    if (latitude == null || longitude == null) continue;
    updates.push({
      trip_id:  vp.trip.tripId.trim(),
      position: { lat: latitude, lng: longitude },
      bearing:  bearing ?? null,
    });
  }
  return { count: feed.entity.length, updates };
}

async function fetchVehicles(db, cityName, url) {
  const t = performance.now();
  try {
    const isJson = JSON_VEHICLES_CITIES.has(cityName);
    const response = await axios.get(url, {
      responseType: isJson ? 'json' : 'arraybuffer',
      timeout: 30000,
      headers: { 'User-Agent': 'gtfs-worker/1.0' },
    });

    const { count, updates } = parseVehicleUpdates(cityName, response);

    const trips = db.collection('daily_trips');
    await trips.updateMany({ position: { $exists: true } }, { $unset: { position: '', bearing: '' } });

    let modifiedCount = 0;
    if (updates.length > 0) {
      const result = await trips.bulkWrite(
        updates.map(u => ({
          updateOne: {
            filter: { trip_id: u.trip_id },
            update: { $set: { position: u.position, bearing: u.bearing } },
          },
        })),
        { ordered: false }
      );
      modifiedCount = result.modifiedCount;
    }

    await log('vehicles_update', {
      city: cityName,
      modified_count: modifiedCount,
      entities: count,
      duration_ms: Math.round(performance.now() - t),
      Note: `Posizioni veicoli per ${cityName}: ${modifiedCount} trip aggiornati su ${count} entità`,
    });

  } catch (err) {
    await logError('vehicles_error', {
      city: cityName,
      error: err.message,
      duration_ms: Math.round(performance.now() - t),
      Note: `Errore posizioni veicoli per ${cityName}: ${err.message}`,
    });
  }
}

async function fetchAlerts(db, cityName, url) {
  const t = performance.now();
  try {
    const isJson = JSON_ALERTS_CITIES.has(cityName);
    const response = await axios.get(url, {
      responseType: isJson ? 'json' : 'arraybuffer',
      timeout: 30000,
      headers: { 'User-Agent': 'gtfs-worker/1.0' },
    });

    const alerts = [];

    if (isJson) {
      for (const entity of response.data?.Entities ?? []) {
        const a = entity.Alert;
        if (!a) continue;
        const route_ids = (a.InformedEntity ?? [])
          .map(ie => ie.RouteId)
          .filter(Boolean);
        if (route_ids.length === 0) continue;
        alerts.push({ alert_id: entity.Id, route_ids });
      }
    } else {
      const feed = FeedMessage.decode(new Uint8Array(response.data));
      for (const entity of feed.entity) {
        const a = entity.alert;
        if (!a) continue;
        const route_ids = (a.informedEntity ?? [])
          .map(ie => ie.routeId)
          .filter(Boolean);
        if (route_ids.length === 0) continue;
        alerts.push({ alert_id: entity.id, route_ids });
      }
    }

    const col = db.collection('alerts');
    await col.deleteMany({});
    if (alerts.length > 0) await col.insertMany(alerts);

    await log('alerts_update', {
      city: cityName,
      count: alerts.length,
      duration_ms: Math.round(performance.now() - t),
      Note: `Alert per ${cityName}: ${alerts.length} inseriti`,
    });

  } catch (err) {
    await logError('alerts_error', {
      city: cityName,
      error: err.message,
      duration_ms: Math.round(performance.now() - t),
      Note: `Errore alert per ${cityName}: ${err.message}`,
    });
  }
}

async function main() {
  const client = new MongoClient(MONGO_URI);
  await client.connect();

  const allCityNames = [...new Set([...CITIES, ...VEHICLE_CITIES, ...ALERT_CITIES].map(c => c.name))];
  await log('realtime_start', {
    realtime_cities: CITIES.map(c => c.name),
    vehicles_cities: VEHICLE_CITIES.map(c => c.name),
    alert_cities:    ALERT_CITIES.map(c => c.name),
    Note: `Worker avviato — realtime: [${CITIES.map(c => c.name).join(', ')}] veicoli: [${VEHICLE_CITIES.map(c => c.name).join(', ')}] alert: [${ALERT_CITIES.map(c => c.name).join(', ')}]`,
  });

  const dbs = Object.fromEntries(allCityNames.map(name => [name, client.db(name)]));

  async function runRealtime() {
    await Promise.allSettled([
      ...CITIES.map(c => fetchRealtime(dbs[c.name], c.name, c.url)),
      ...VEHICLE_CITIES.map(c => fetchVehicles(dbs[c.name], c.name, c.url)),
    ]);
  }

  async function runAlerts() {
    await Promise.allSettled(
      ALERT_CITIES.map(c => fetchAlerts(dbs[c.name], c.name, c.url))
    );
  }

  if (isOperatingHours()) { await runRealtime(); await runAlerts(); }
  setInterval(() => { if (isOperatingHours()) runRealtime(); }, 30000);
  setInterval(() => { if (isOperatingHours()) runAlerts(); }, 600000);
}

main().catch(async (err) => {
  await logError('realtime_fatal', { error: err.message, Note: `Errore fatale worker: ${err.message}` });
  process.exit(1);
});

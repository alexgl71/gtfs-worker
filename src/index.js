// Env vars: REMOTE_URI + REALTIME_URL_* + VEHICLES_URL_* + ALERTS_URL_*
// TripUpdates ogni 30s, Vehicles ogni 30s, Alerts ogni 10min.
// Attivo solo dalle 07:00 alle 23:00 UTC+2.

require('dotenv').config();
const { MongoClient } = require('mongodb');
const { log, logError } = require('./logger');
const { fetchTripUpdates } = require('./tripUpdates');
const { fetchVehicles }    = require('./vehicles');
const { fetchAlerts }      = require('./alerts');

const MONGO_URI = process.env.REMOTE_URI;
if (!MONGO_URI) { console.error('REMOTE_URI non impostato'); process.exit(1); }

function cityName(key, prefix) {
  const raw = key.replace(prefix, '');
  return raw.charAt(0) + raw.slice(1).toLowerCase();
}

const REALTIME_CITIES = Object.entries(process.env)
  .filter(([k]) => k.startsWith('REALTIME_URL_'))
  .map(([k, url]) => ({ name: cityName(k, 'REALTIME_URL_'), url }));

const VEHICLE_CITIES = Object.entries(process.env)
  .filter(([k]) => k.startsWith('VEHICLES_URL_'))
  .map(([k, url]) => ({ name: cityName(k, 'VEHICLES_URL_'), url }));

const ALERT_CITIES = Object.entries(process.env)
  .filter(([k]) => k.startsWith('ALERTS_URL_'))
  .map(([k, url]) => ({ name: cityName(k, 'ALERTS_URL_'), url }));

if (REALTIME_CITIES.length === 0 && VEHICLE_CITIES.length === 0 && ALERT_CITIES.length === 0) {
  console.error('Nessuna REALTIME_URL_*, VEHICLES_URL_* o ALERTS_URL_* configurata');
  process.exit(1);
}

function isOperatingHours() {
  const hourUTC2 = (new Date().getUTCHours() + 2) % 24;
  return hourUTC2 >= 7 && hourUTC2 < 23;
}

async function main() {
  const client = new MongoClient(MONGO_URI);
  await client.connect();

  const allNames = [...new Set([...REALTIME_CITIES, ...VEHICLE_CITIES, ...ALERT_CITIES].map(c => c.name))];
  const dbs = Object.fromEntries(allNames.map(name => [name, client.db(name)]));

  console.log(`[worker] Avviato — realtime: [${REALTIME_CITIES.map(c=>c.name).join(', ')}] veicoli: [${VEHICLE_CITIES.map(c=>c.name).join(', ')}] alert: [${ALERT_CITIES.map(c=>c.name).join(', ')}]`);
  await log('realtime_start', {
    realtime_cities: REALTIME_CITIES.map(c => c.name),
    vehicles_cities: VEHICLE_CITIES.map(c => c.name),
    alert_cities:    ALERT_CITIES.map(c => c.name),
    Note: `Worker avviato — realtime: [${REALTIME_CITIES.map(c => c.name).join(', ')}] veicoli: [${VEHICLE_CITIES.map(c => c.name).join(', ')}] alert: [${ALERT_CITIES.map(c => c.name).join(', ')}]`,
  });

  async function runRealtime() {
    await Promise.allSettled([
      ...REALTIME_CITIES.map(c => fetchTripUpdates(dbs[c.name], c.name, c.url)),
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

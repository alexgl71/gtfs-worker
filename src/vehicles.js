const axios = require('axios');
const GtfsRealtime = require('gtfs-realtime-bindings');
const FeedMessage = GtfsRealtime.transit_realtime.FeedMessage;
const { log, logError } = require('./logger');

const JSON_CITIES = new Set(['Bari']);

function parse(cityName, response) {
  if (JSON_CITIES.has(cityName)) {
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
    const isJson = JSON_CITIES.has(cityName);
    const response = await axios.get(url, {
      responseType: isJson ? 'json' : 'arraybuffer',
      timeout: 30000,
      headers: { 'User-Agent': 'gtfs-worker/1.0' },
    });

    const { count, updates } = parse(cityName, response);

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

module.exports = { fetchVehicles };

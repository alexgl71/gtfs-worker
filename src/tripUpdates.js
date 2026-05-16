const axios = require('axios');
const GtfsRealtime = require('gtfs-realtime-bindings');
const FeedMessage = GtfsRealtime.transit_realtime.FeedMessage;
const { logError } = require('./logger');

const JSON_CITIES = new Set(['Bari']);

function parse(cityName, response) {
  if (JSON_CITIES.has(cityName)) {
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

async function fetchTripUpdates(db, cityName, url) {
  const t = performance.now();
  try {
    const isJson = JSON_CITIES.has(cityName);
    const response = await axios.get(url, {
      responseType: isJson ? 'json' : 'arraybuffer',
      timeout: 60000,
      headers: { 'User-Agent': 'gtfs-worker/1.0' },
    });

    const { count, updates } = parse(cityName, response);

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

    const ms = Math.round(performance.now() - t);
    console.log(`[tripUpdates] ${cityName} — ${modifiedCount}/${count} trip aggiornati (${ms}ms)`);

  } catch (err) {
    const ms = Math.round(performance.now() - t);
    console.log(`[tripUpdates] ${cityName} — ERRORE: ${err.message} (${ms}ms)`);
    await logError('realtime_error', {
      city: cityName,
      error: err.message,
      duration_ms: ms,
      Note: `Errore realtime per ${cityName}: ${err.message}`,
    });
  }
}

module.exports = { fetchTripUpdates };

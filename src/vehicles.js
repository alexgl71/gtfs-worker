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
      vehicles: entities
        .filter(e => e.Vehicle?.Trip?.TripId && e.Vehicle.Position?.Latitude != null)
        .map(e => ({
          id:           e.Id ?? null,
          label:        e.Vehicle.Vehicle?.Label ?? null,
          trip_id:      e.Vehicle.Trip.TripId.trim(),
          route_id:     e.Vehicle.Trip.RouteId ?? null,
          direction_id: e.Vehicle.Trip.DirectionId?.toString() ?? null,
          start_time:   e.Vehicle.Trip.StartTime ?? null,
          start_date:   e.Vehicle.Trip.StartDate ?? null,
          latitude:     e.Vehicle.Position.Latitude,
          longitude:    e.Vehicle.Position.Longitude,
          stop_sequence: e.Vehicle.CurrentStopSequence ?? null,
          stop_id:      e.Vehicle.StopId ?? null,
          timestamp:    e.Vehicle.Timestamp ? e.Vehicle.Timestamp * 1000 : null,
          bearing:      e.Vehicle.Position.Bearing ?? null,
        })),
    };
  }

  const feed = FeedMessage.decode(new Uint8Array(response.data));
  const vehicles = [];
  for (const entity of feed.entity) {
    const vp = entity.vehicle;
    if (!vp?.trip?.tripId || !vp.position) continue;
    const { latitude, longitude, bearing } = vp.position;
    if (latitude == null || longitude == null) continue;
    vehicles.push({
      id:           entity.id ?? null,
      label:        vp.vehicle?.label ?? null,
      trip_id:      vp.trip.tripId.trim(),
      route_id:     vp.trip.routeId ?? null,
      direction_id: vp.trip.directionId?.toString() ?? null,
      start_time:   vp.trip.startTime ?? null,
      start_date:   vp.trip.startDate ?? null,
      latitude,
      longitude,
      stop_sequence: vp.currentStopSequence ?? null,
      stop_id:      vp.stopId ?? null,
      timestamp:    vp.timestamp ? Number(vp.timestamp) * 1000 : null,
      bearing:      bearing ?? null,
    });
  }
  return { count: feed.entity.length, vehicles };
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

    const { count, vehicles } = parse(cityName, response);

    // Aggiorna daily_trips con position+bearing (usato dal router)
    const trips = db.collection('daily_trips');
    await trips.updateMany({ position: { $exists: true } }, { $unset: { position: '', bearing: '' } });
    if (vehicles.length > 0) {
      await trips.bulkWrite(
        vehicles.map(v => ({
          updateOne: {
            filter: { trip_id: v.trip_id },
            update: { $set: { position: { lat: v.latitude, lng: v.longitude }, bearing: v.bearing } },
          },
        })),
        { ordered: false }
      );
    }

    // Salva la collection vehicles completa per l'endpoint byroute
    const col = db.collection('vehicles');
    await col.deleteMany({});
    if (vehicles.length > 0) {
      await col.insertMany(vehicles);
      await col.createIndex({ route_id: 1 });
    }

    await log('vehicles_update', {
      city: cityName,
      count: vehicles.length,
      entities: count,
      duration_ms: Math.round(performance.now() - t),
      Note: `Posizioni veicoli per ${cityName}: ${vehicles.length} veicoli su ${count} entità`,
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

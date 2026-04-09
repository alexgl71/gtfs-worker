const axios = require('axios');
const GtfsRealtime = require('gtfs-realtime-bindings');
const FeedMessage = GtfsRealtime.transit_realtime.FeedMessage;
const { log, logError } = require('./logger');

const JSON_CITIES = new Set(['Bari']);

const CAUSE_NAMES = {
  0: 'UnknownCause', 1: 'OtherCause',       2: 'TechnicalProblem', 3: 'Strike',
  4: 'Demonstration', 5: 'Accident',         6: 'Holiday',          7: 'Weather',
  8: 'Maintenance',   9: 'Construction',    10: 'PoliceActivity',   11: 'MedicalEmergency',
};
const EFFECT_NAMES = {
  0: 'NoService',    1: 'ReducedService',   2: 'SignificantDelays', 3: 'Detour',
  4: 'AdditionalService', 5: 'ModifiedService', 6: 'OtherEffect',  7: 'UnknownEffect',
  8: 'StopMoved',    9: 'NoEffect',        10: 'AccessibilityIssue',
};

async function fetchAlerts(db, cityName, url) {
  const t = performance.now();
  try {
    const isJson = JSON_CITIES.has(cityName);
    const response = await axios.get(url, {
      responseType: isJson ? 'json' : 'arraybuffer',
      timeout: 30000,
      headers: { 'User-Agent': 'gtfs-worker/1.0' },
    });

    const now = new Date();
    const alerts = [];

    if (isJson) {
      for (const entity of response.data?.Entities ?? []) {
        const a = entity.Alert;
        if (!a) continue;
        const route_ids = (a.InformedEntity ?? []).map(ie => ie.RouteId).filter(Boolean);
        const stop_ids  = (a.InformedEntity ?? []).map(ie => ie.StopId).filter(Boolean);
        if (route_ids.length === 0) continue;
        alerts.push({
          alert_id:       entity.Id,
          cause:          a.Cause ?? null,
          effect:         a.Effect ?? null,
          severity_level: a.SeverityLevel ?? null,
          active_periods: (a.ActivePeriod ?? []).map(p => ({ start: p.Start ?? null, end: p.End ?? null })),
          url:            a.Url?.Translation?.[0]?.Text ?? null,
          header:         a.HeaderText?.Translation?.[0]?.Text ?? null,
          description:    a.DescriptionText?.Translation?.[0]?.Text ?? null,
          route_ids,
          stop_ids,
          created_date:   now,
          last_updated:   now,
        });
      }
    } else {
      const feed = FeedMessage.decode(new Uint8Array(response.data));
      const firstText = (ts) => ts?.translation?.[0]?.text ?? null;
      for (const entity of feed.entity) {
        const a = entity.alert;
        if (!a) continue;
        const route_ids = (a.informedEntity ?? []).map(ie => ie.routeId).filter(Boolean);
        const stop_ids  = (a.informedEntity ?? []).map(ie => ie.stopId).filter(Boolean);
        if (route_ids.length === 0) continue;
        alerts.push({
          alert_id:       entity.id,
          cause:          CAUSE_NAMES[a.cause] ?? null,
          effect:         EFFECT_NAMES[a.effect] ?? null,
          severity_level: null,
          active_periods: (a.activePeriod ?? []).map(p => ({
            start: p.start != null ? (typeof p.start === 'object' ? p.start.low : p.start) : null,
            end:   p.end   != null ? (typeof p.end   === 'object' ? p.end.low   : p.end)   : null,
          })),
          url:            firstText(a.url),
          header:         firstText(a.headerText),
          description:    firstText(a.descriptionText),
          route_ids,
          stop_ids,
          created_date:   now,
          last_updated:   now,
        });
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

module.exports = { fetchAlerts };

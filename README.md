# gtfs-worker

Background worker che recupera dati GTFS-RT (realtime) da feed pubblici e li salva su MongoDB Atlas. Gira su Render come Background Worker.

## Cosa fa

Ogni **30 secondi** (solo tra le 07:00 e le 23:00 UTC+2):
- Scarica i feed **TripUpdates** e aggiorna i ritardi sui documenti `daily_trips`
- Scarica i feed **VehiclePositions** e aggiorna posizioni/bearing su `daily_trips` + sostituisce la collection `vehicles`

Ogni **10 minuti** (solo tra le 07:00 e le 23:00 UTC+2):
- Scarica i feed **Alerts** e sostituisce la collection `alerts`

Tutti gli eventi (successo ed errore) vengono loggati su MongoDB in `general/logs` e `general/errors`.

## Struttura

```
src/
  index.js        # entrypoint: connessione MongoDB, scheduler, orario operativo
  tripUpdates.js  # fetch + parsing TripUpdates → aggiorna daily_trips.delay_sec
  vehicles.js     # fetch + parsing VehiclePositions → aggiorna daily_trips + collection vehicles
  alerts.js       # fetch + parsing Alerts → sostituisce collection alerts
  logger.js       # log/logError su general/logs e general/errors
```

## Variabili d'ambiente

| Variabile | Descrizione |
|---|---|
| `REMOTE_URI` | URI di connessione MongoDB Atlas (obbligatoria) |
| `REALTIME_URL_<CITTÀ>` | URL feed GTFS-RT TripUpdates per la città (es. `REALTIME_URL_MILANO`) |
| `VEHICLES_URL_<CITTÀ>` | URL feed GTFS-RT VehiclePositions per la città |
| `ALERTS_URL_<CITTÀ>` | URL feed GTFS-RT Alerts per la città |

Il nome della città viene ricavato dal suffisso della variabile (es. `REALTIME_URL_MILANO` → città `Milano`). Ogni città corrisponde a un database MongoDB con lo stesso nome.

## MongoDB — struttura dei dati

### `<città>/daily_trips`
Collection statica (gestita da altro sistema). Il worker la aggiorna con:
- `delay_sec` — ritardo in secondi dall'ultimo TripUpdate (rimosso se assente)
- `position` `{ lat, lng }` + `bearing` — posizione veicolo (rimossi se assenti)

### `<città>/vehicles`
Sostituita ad ogni ciclo. Ogni documento rappresenta un veicolo in tempo reale:

| Campo | Tipo | Descrizione |
|---|---|---|
| `id` | string | ID entità feed |
| `label` | string | Etichetta veicolo |
| `trip_id` | string | ID corsa |
| `route_id` | string | ID linea |
| `direction_id` | string | Direzione (`"0"` o `"1"`) |
| `start_time` | string | Orario partenza corsa |
| `start_date` | string | Data partenza corsa |
| `latitude` | number | Latitudine |
| `longitude` | number | Longitudine |
| `bearing` | number | Direzione in gradi |
| `stop_sequence` | number\|null | Sequenza fermata corrente |
| `stop_id` | string\|null | ID fermata corrente |
| `timestamp` | number\|null | Timestamp posizione (ms) |

Indice: `route_id` (per query byroute).

### `<città>/alerts`
Sostituita ogni 10 minuti. Ogni documento rappresenta un'allerta attiva:

| Campo | Tipo | Descrizione |
|---|---|---|
| `alert_id` | string | ID entità feed |
| `cause` | string | Causa (es. `"Strike"`, `"Accident"`) |
| `effect` | string | Effetto (es. `"Detour"`, `"ReducedService"`) |
| `severity_level` | string\|null | Livello severità (solo feed JSON) |
| `active_periods` | array | `[{ start, end }]` in secondi Unix |
| `header` | string\|null | Titolo allerta |
| `description` | string\|null | Descrizione allerta |
| `url` | string\|null | Link informazioni |
| `route_ids` | array | Linee coinvolte |
| `stop_ids` | array | Fermate coinvolte |
| `created_date` | Date | Data inserimento |
| `last_updated` | Date | Data aggiornamento |

### `general/logs`
Log operativi. Tipi: `realtime_start`, `realtime_update`, `vehicles_update`, `alerts_update`.

### `general/errors`
Errori per singola città. Tipi: `realtime_error`, `vehicles_error`, `alerts_error`, `realtime_fatal`.

## Formati feed supportati

- **Protobuf** — formato standard GTFS-RT binario (default)
- **JSON** — formato proprietario usato da Bari (rilevato automaticamente per le città nel set `JSON_CITIES`)

## Deploy su Render

Tipo servizio: **Background Worker**  
Build command: `npm install`  
Start command: `node src/index.js`

Il processo logga su MongoDB, non su stdout — la console Render mostrerà solo errori fatali all'avvio.

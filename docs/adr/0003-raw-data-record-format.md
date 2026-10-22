# ADR 0003: Raw data record format

Status: Accepted

## Context

Raw market data is the input to every arm and to the evaluation. Once collection starts, the format is fixed: changing it means migrating every file already collected, and any hash already published would stop matching. ADR 0002 requires that each record carries the time it became available. This record fixes how raw data is stored so that both requirements hold from the first day.

## Decision

### Envelope

Every raw record is one line of JSON with these fields, in this order:

| Field | Type | Meaning |
|---|---|---|
| `available_ts` | int | UTC wall clock in nanoseconds when the collector received the message (WebSocket) or finished reading the response (REST) |
| `mono_ns` | int | Monotonic clock, read together with `available_ts` |
| `boot_id` | str | Identifies the machine boot, so `mono_ns` values can be compared |
| `conn_id` | str | One UUID per WebSocket connection; `"rest"` for REST; `"event"` for collector events |
| `source` | str | `live`, `backfill` (gap filled from REST while running) or `history` (imported, never seen live) |
| `kind` | str | `ws`, `rest` or `event` |
| `stream` | str | WebSocket stream name; for REST, method, path and query string; for events, the event type |
| `http_status` | int or null | REST only |
| `raw` | str | The message or response body exactly as received, never parsed or rewritten |

`event_ts` is not stored in the envelope. It is parsed from `raw` when the data is read: the close time `T` for candles, the event time `E` for perpetual depth, mark price and liquidations. Spot depth messages have no event time; they are ordered by `lastUpdateId` and timed by `available_ts`.

For backfilled data, `available_ts` is the time the REST response was actually received, which is after the candle closed. It is never estimated.

### Files

- Path: `{market}/{channel}/{YYYYMMDD}/{HH}.jsonl`, relative to the data root, split by the UTC hour of `available_ts`. `market` is `spot`, `um` (USD-margined perpetuals) or `sys` (collector events).
- A file is written as `HH.jsonl.open` and renamed to `HH.jsonl` after its hour ends. A channel with no data in an hour while the collector was running still gets an empty file.
- Sealing compresses each closed file to `HH.jsonl.gz` (gzip, header `mtime` 0) and then writes the hour's manifest, `manifest/{YYYYMMDD}/{HH}.json`. An hour is sealed once its manifest exists. Sealed files and manifests are never modified.
- The manifest lists, for each file, its relative path, the sha256 of the compressed bytes, its size, its line count, and the smallest and largest `available_ts`. Manifests are written with sorted keys and no whitespace. Integers are written exactly. Nanosecond timestamps exceed 2^53, so this is not strict RFC 8785. Hashes are always taken over file bytes, never over re-serialized JSON.

### Deduplication

Raw files may contain duplicates (overlapping connections during rotation, backfills that overlap live data). Readers deduplicate by these keys and keep the copy with the smaller `available_ts`:

| Channel | Key |
|---|---|
| Spot depth | stream + `lastUpdateId` |
| Perpetual depth | stream + the second of `E` |
| Candles, live and backfilled | market + symbol + interval + open time |
| Mark price | stream + `E` |
| Liquidations | the `o` object + `E` |
| Open interest | symbol + `time` |

### Universe

The 20 coins that are collected and traded are selected at 00:00 UTC on the first day of each month, and once at first launch:

1. Candidates are spot pairs with `status=TRADING` and `quoteAsset=USDT`, excluding stablecoins, other pegged assets and leveraged tokens, as listed in `config/collect.toml`.
2. Rank by the sum of quote volume over the 30 complete UTC days before the selection time, and keep the top 20.
3. Each coin's perpetual is the USDT-margined `PERPETUAL` contract whose base asset matches after removing a quantity prefix (`1000000`, `1000`, `1M`), for example `PEPEUSDT` and `1000PEPEUSDT`.
4. The result, its inputs and the time it was written are stored in `universe/YYYY-MM.json`. A coin halted or delisted mid-month is not replaced.

## Consequences

- Raw data can be re-read exactly as received, so parsing bugs can be fixed later without losing anything.
- Every collected file can be checked against a published hash.
- Readers have to parse `raw` and deduplicate. That cost is paid in code, once.
- Changing any part of this record means migrating collected data. Changes to the universe rule, the file layout or the envelope need a new ADR.

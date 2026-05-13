# Trades Benchmark Fixture

This directory contains a compressed newline-delimited JSON fixture for larger
single-file storage benchmarks.

## Fixture

| File | Uncompressed | Compressed | Documents | SHA-256 |
| --- | ---: | ---: | ---: | --- |
| `trades.json.zst` | 243,000,227 bytes | 4.58 MiB | 1,000,001 | `a1bcd33842e1007c0a79fa36d1580b72dced3b6c7619c5082b1d255efb94b726` |

The uncompressed source hash used for the baseline run was:

```text
e54467caaa9ddfa432dcabb0910346f4601d8a44cb2b8d25ce846ae880af1087  trades.json
```

Decompress it into `/tmp` for local runs:

```sh
zstd -d -f benchmarks/trades.json.zst -o /tmp/mqlite-bench-trades.json
```

The fixture shape is:

```json
{"_id":{"$oid":"5597a1617df886b33f839f9a"},"details":{"asks":[110.07,110.12,110.3],"bids":[109.9,109.88,109.7,109.5],"lag":0,"system":"abc"},"price":110,"shares":200,"ticker":"abcd","time":{"$date":"2012-03-02T22:00:00.000Z"}}
```

Most documents include `ticker`; many include `ticket`; `_id` is an ObjectId.

## Current Baseline

Baseline date: 2026-05-12.

Environment:

| Engine | Version / commit | Notes |
| --- | --- | --- |
| MongoDB standalone | `mongod` 8.2.9, `mongoimport` 100.16.1 | `mongod --dbpath /tmp/mqlite-mongod-baseline --bind_ip 127.0.0.1 --port 27018` |
| mqlite | `4f91c7d` | `target/debug/mqlite`, local broker IPC |

Results:

| Operation | MongoDB standalone | mqlite current | Gap |
| --- | ---: | ---: | ---: |
| Import 1,000,001 docs, batch 1000 | 27.87s, 35,874 docs/s | 323.43s, 3,092 docs/s | mqlite 11.6x slower |
| Build `ticker_1` and `ticket_1` | 3.00s server-side, 8.92s including `mongosh` startup | 341.63s command wall time | mqlite 114x slower than MongoDB server-side index build |
| Count all docs | p50 216.42ms | 2.20s cold CLI on WAL-backed indexed file via metadata path | mqlite 10.2x slower than MongoDB p50, but no full record hydration |
| `_id` point read, 1000 iterations | p50 0.72ms, p95 1.08ms | 1.61s cold CLI for first fixture `_id` on WAL-backed indexed file | mqlite remains far slower than MongoDB warm p50, but avoids full record hydration |
| `ticket: "z300"` indexed point read, 1000 iterations | p50 1.33ms, p95 4.44ms | 1.09s cold CLI pending-WAL equality lookup | mqlite cold CLI remains far slower than MongoDB warm p50, but avoids full hydration |
| `ticket: "z300"` indexed count, 10 iterations | p50 2.44ms, result 2500 | 0.10s cold CLI pending-WAL index-frequency count, result 2500 | mqlite cold CLI remains far slower than MongoDB warm p50, but avoids full hydration |

Storage size after MongoDB import and secondary indexes:

| Metric | MongoDB standalone |
| --- | ---: |
| Logical document size | 231,000,214 bytes |
| Collection storage size | 33,034,240 bytes |
| Data path size | 91 MiB |

Important mqlite caveat from this run: interrupting the long secondary-index
build left the test file at the last published checkpoint instead of the full
imported state. The visible checkpoint contained 236,000 records. Treat the
full-import timing as an import throughput data point, but do not reuse that
interrupted `.mongodb` file for read benchmarks.

## Reproducing MongoDB Baseline

Start standalone `mongod`:

```sh
rm -rf /tmp/mqlite-mongod-baseline
mkdir -p /tmp/mqlite-mongod-baseline
mongod \
  --dbpath /tmp/mqlite-mongod-baseline \
  --bind_ip 127.0.0.1 \
  --port 27018 \
  --setParameter diagnosticDataCollectionEnabled=false
```

Import:

```sh
mongoimport \
  --uri mongodb://127.0.0.1:27018/market \
  --collection trades \
  --drop \
  --file /tmp/mqlite-bench-trades.json \
  --numInsertionWorkers 1 \
  --batchSize 1000
```

Create indexes:

```sh
mongosh mongodb://127.0.0.1:27018/market --quiet --eval '
const started = Date.now();
db.trades.createIndexes([{ ticker: 1 }, { ticket: 1 }]);
printjson({ elapsedMs: Date.now() - started, indexes: db.trades.getIndexes().map(i => i.name) });
'
```

Point-read benchmark:

```sh
mongosh mongodb://127.0.0.1:27018/market --quiet --eval '
const coll = db.trades;
function timed(label, fn, iterations) {
  const samples = [];
  let result;
  for (let i = 0; i < iterations; i++) {
    const started = process.hrtime.bigint();
    result = fn();
    samples.push(Number(process.hrtime.bigint() - started) / 1e6);
  }
  samples.sort((a, b) => a - b);
  const pick = p => samples[Math.min(samples.length - 1, Math.round((p / 100) * (samples.length - 1)))];
  return { label, iterations, firstMs: samples[0], p50Ms: pick(50), p95Ms: pick(95), maxMs: samples[samples.length - 1], result };
}
print(JSON.stringify({
  count: timed("countDocuments", () => coll.countDocuments({}), 3),
  idPoint: timed("_id point", () => coll.find({ _id: ObjectId("5597a1627df886b33f839f9b") }).limit(1).toArray().length, 1000),
  ticketPoint: timed("ticket z300", () => coll.find({ ticket: "z300" }).limit(1).toArray().length, 1000),
  ticketCount: timed("ticket z300 count", () => coll.countDocuments({ ticket: "z300" }), 10)
}));
'
```

## Reproducing mqlite Import Baseline

The current first-class import harness keeps one broker connection open and
streams the committed fixture directly, including `.zst` decompression:

```sh
rm -f /tmp/mqlite-trades.mongodb
target/debug/mqlite bench trades-import \
  --file /tmp/mqlite-trades.mongodb \
  --fixture benchmarks/trades.json.zst \
  --batch-size 1000 \
  --reset \
  --idle-shutdown-secs 3600
```

It verifies the final count, then prints structured JSON with document count,
batch count, wall time, docs/sec, startup time, parse time,
count-verification time, insert latency percentiles, and file/WAL/storage
counters. Add `--checkpoint` when the run should force and time broker
checkpoint publication before reporting storage counters.

After creating `ticker_1` and `ticket_1`, use `trades-read` to measure warm
read latency over one broker connection:

```sh
target/debug/mqlite bench trades-read \
  --file /tmp/mqlite-trades.mongodb \
  --reads 100 \
  --count-reads 10
```

It reports startup time, warm `ticket:"z300"` and `ticker:"abcd"` point-read
p50/p95/max, `ticket:"z300"` count p50/p95/max, first-query broker debug
metadata, and storage counters.

The older baseline below used a Python loop that spawned `mqlite command` for
each batch against a long-lived broker. Keep it as the comparison point for the
2026-05-12 baseline.

Start a long-lived broker before importing. Auto-spawning a broker per command
is not representative for this dataset.

```sh
rm -f /tmp/mqlite-trades.mongodb
target/debug/mqlite serve --file /tmp/mqlite-trades.mongodb --idle-shutdown-secs 3600
```

Then run batched inserts through `mqlite command`:

```sh
python3 - <<'PY'
import json, pathlib, subprocess, time

src = pathlib.Path("/tmp/mqlite-bench-trades.json")
dbfile = "/tmp/mqlite-trades.mongodb"
exe = "target/debug/mqlite"
db = "market"
coll = "trades"
batch_size = 1000

def command(doc):
    payload = json.dumps(doc, separators=(",", ":"))
    subprocess.run(
        [exe, "command", "--file", dbfile, "--db", db, "--idle-shutdown-secs", "3600"],
        input=payload,
        text=True,
        check=True,
    )

started = time.perf_counter()
command({"create": coll})
count = 0
batch = []
with src.open() as handle:
    for line in handle:
        if not line.strip():
            continue
        batch.append(json.loads(line))
        if len(batch) == batch_size:
            command({"insert": coll, "documents": batch})
            count += len(batch)
            batch.clear()
if batch:
    command({"insert": coll, "documents": batch})
    count += len(batch)

elapsed = time.perf_counter() - started
print(json.dumps({"documents": count, "elapsedMs": elapsed * 1000, "docsPerSec": count / elapsed}, indent=2))
PY
```

Current mqlite import result from the local run:

```json
{
  "documents": 1000001,
  "batchSize": 1000,
  "elapsedMs": 412172.89406305645,
  "docsPerSec": 2426.1687617114735
}
```

First-class harness results from this patch:

| Operation | Previous Python loop | `bench trades-import` | Notes |
| --- | ---: | ---: | --- |
| Import fixture, batch 1000 | 412.17s, 2,426 docs/s | 323.43s, 3,092 docs/s with live count validation | Single broker connection removed per-batch client spawn overhead and the live count matched 1,000,001 documents. The file remains WAL-backed until checkpointed. The latest run keeps WAL metadata outside compressed mutation payloads and is the fastest full import measured so far. |
| Build `ticker_1` and `ticket_1` after import | did not complete within 318.89s; interrupted near 11.8 GiB RSS in the original probe | 341.63s command wall time; earlier observed broker RSS roughly 4.0 GiB at 3:47 | Existing-record index builds now collect keys linearly, sort once, and apply the validation-built indexes instead of rebuilding them again during mutation apply. The command completed and reopen metadata reported 3 indexes and 3,000,003 entries. It remains far slower than MongoDB and still needs a page-backed or external-sort bulk builder. |
| Count all documents after import and index build | manually stopped after 107.73s after falling back to dirty-WAL record overlay | 2.20s real, 7.7 MB CLI max RSS | Empty-filter `count` now answers from metadata folded across checkpoint and WAL prefixes. Debug output reported `readPath=metadataCount` and returned `n=1,000,001`. |
| `_id` point read after import and index build | manually stopped after 40.84s on the generic dirty-WAL overlay path | 1.61s real, 7.8 MB CLI max RSS | Simple `_id` equality can use the pending-WAL id lookup. Debug output reported `readPath=pendingWalIdLookup` while scanning 1002 WAL records. The measured id is the first fixture `_id`; the earlier MongoDB baseline id was not a useful mqlite fixture probe. |
| `ticket:"z300"` point read after import and index build | did not return within 30s on the generic hydrated path | 253.92ms warm p50, 263.92ms p95 over 100 reads | `bench trades-read` reported `readPath=pendingWalEqualityLookup` and returned the first matching document without opening mutable storage. This is still far slower than MongoDB's 1.33ms p50 and still dirty-WAL-backed rather than clean checkpointed secondary-index execution. |
| `ticker:"abcd"` point read after import and index build | not previously measured | 248.34ms warm p50, 272.64ms p95 over 100 reads | `bench trades-read` reported `readPath=pendingWalEqualityLookup`; this covers ticker read latency but is still dirty-WAL-backed rather than clean checkpointed secondary-index execution. |
| `ticket:"z300"` count after import and index build | manually stopped after 40.82s on the generic dirty-WAL overlay path; later streaming count stopped after 165.03s | 49.51ms warm p50, 51.18ms p95 over 10 counts | Fresh `createIndexes` WAL frames carry index value-frequency metadata, and the metadata prefix is stored outside compressed mutation payloads. Debug output reported `readPath=pendingWalEqualityCount`, scanned 1,002 WAL metadata records, and returned `n=2500` without full collection hydration. |
| Forced checkpoint after import | not measured | timed out at 360.93s, 11.2 GB max RSS | The foreground checkpoint path still does not complete on the full fixture. After timeout, `mqlite info` still reported checkpoint LSN 0 and 1,002 WAL records, so the file did not appear clean after the interrupted checkpoint. |
| WAL-backed metadata fold, full fixture | not measured | 0.07s real, 9.6 MB max RSS | New WAL frames keep the metadata prefix outside compressed mutation payloads, so `mqlite info` folded 1,002 WAL records and reported 1,000,001 records plus 3,000,003 index entries without deserializing the full mutation payloads. |

The successful live-count result is checked in at
`benchmarks/results/2026-05-13-trades-import-live-validated.json`. The earlier
failed count-validation result is retained at
`benchmarks/results/2026-05-13-trades-import-validation-failed.json`.
The fresh WAL metadata prefix run is checked in at
`benchmarks/results/2026-05-13-trades-import-wal-metadata.json`.
The completed full-fixture secondary-index run is checked in at
`benchmarks/results/2026-05-13-trades-secondary-indexes.json`.
The metadata-backed count-all run is checked in at
`benchmarks/results/2026-05-13-trades-count-metadata.json`.
The pending-WAL `_id` lookup run is checked in at
`benchmarks/results/2026-05-13-trades-id-lookup.json`.
The bounded but incomplete `ticket:"z300"` streaming-count probe is checked in at
`benchmarks/results/2026-05-13-trades-ticket-count-streaming-stopped.json`.
The fresh 10k `ticket:"z300"` index-frequency smoke is checked in at
`benchmarks/results/2026-05-13-trades-10k-ticket-count-index-frequency.json`.
The full-fixture `ticket:"z300"` index-frequency run is checked in at
`benchmarks/results/2026-05-13-trades-full-ticket-count-index-frequency.json`.
The full-fixture `ticket:"z300"` pending-WAL point-read run is checked in at
`benchmarks/results/2026-05-13-trades-full-ticket-find-pending-wal.json`.
The warm `bench trades-read` run is checked in at
`benchmarks/results/2026-05-13-trades-read-warm-pending-wal.json`.
The fresh full-fixture forced-checkpoint timeout is checked in at
`benchmarks/results/2026-05-13-trades-checkpoint-timeout-360s.json`.

## Next Work Items

- Make full checkpoint publication bounded enough for
  `mqlite bench trades-import --checkpoint` to complete on the full fixture.
- Make mqlite index builds page-backed and more tightly bounded in memory; the
  current full-fixture secondary-index build now completes, but still takes
  331.65s and reached roughly 4.0 GiB observed broker RSS during the run.
- Add safe benchmark cleanup/checkpoint handling so interrupting long operations
  cannot make benchmark files look successfully complete when only an older
  checkpoint is visible.
- Clean checkpointed secondary-index reads still need coverage; the current
  `ticket`/`ticker` point-read and `ticket` count measurements are warm
  dirty-WAL-backed runs.

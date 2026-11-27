# PostgreSQL exploratory workload baseline

Historical measurement from core commit `856b2b3ee775980a9864e36f6d021e7f88be5a95`,
before extracting this repository. The recorded source fingerprint describes that
original layout. Current commands below use the standalone paths; a new run emits
a new fingerprint for installed core source, adapter source and dependency files.

This is an observed **single-consumer baseline**, measured on 2026-11-27T13:23:09Z
using the public Cycle queue API. It is evidence that the measured workflows ran
with the stated dataset and settings. It is not an SLO, a maximum-throughput
claim, a production capacity recommendation, or a concurrency measurement.

The runnable source is [QueueWorkload.php](../../benchmarks/QueueWorkload.php).
The [complete JSON result](../../benchmarks/results/postgres-2026-11-27.json)
contains unrounded sample summaries, operation counts, configuration, fingerprints,
and the actual `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` results.

## Environment and isolation

| Property | Observed value |
| --- | --- |
| PHP | 8.5.13 with `pdo_pgsql` |
| Cycle Database | 2.24.0 |
| PostgreSQL | 17.12, 64-bit |
| OS | Linux 6.18.57 x86_64 |
| Server / client encoding | UTF8 / UTF8 |
| `fsync` | on |
| `synchronous_commit` | on |
| `full_page_writes` | on |
| `wal_sync_method` | fdatasync |
| Transaction isolation | read committed |
| Block size | 8192 bytes |
| `shared_buffers` | 128 MB |
| Transport | local TCP, one sequential consumer |
| Payload body | 256 bytes; default JSON/base64 codec |
| Warmup | 20 publish → claim → acknowledge workflows |

The database cluster and data directory were created specifically for this run.
The runner requires an explicit `QUEUE_BENCH_DSN`; it never reads `QUEUE_TEST_DSN`
or an application DSN. It creates a random schema, uses only its own tables,
closes its receivers and connections, and drops the schema in `finally`.
A separate database query after the final measured run found **zero remaining
benchmark schemas**; the earlier smoke/trial run was also cleaned. The temporary
PostgreSQL server was stopped.

The normal workload used one queue connection. A separate connection captured
claim SQL for query-plan inspection, and an administrative connection owned
schema creation and removal. Query capture was disabled on the timed connection.
No pre-existing databases or tables were changed.

The measured production-source fingerprint was unchanged across the run:

```text
512312a5936a7a8cb74619db8e362bdc977e3c19594f42bc8068e8429c087e08
```

The runner's SHA-256 was:

```text
dc502825b4afd926103e10b8ad2ca10b6ac7cb951ff928f4102bac573fac64a1
```

These hashes identify the measured working tree; this was not a release-tag
measurement. The production-source hash includes the core and Cycle PHP source
files, Cycle SQL resources, and the root `composer.lock` in sorted path order.
The implementation defines the fingerprint inputs and reports before/after
fingerprints on every run.

## Workloads and elapsed time

| Scenario | Operations and dataset | Observed elapsed |
| --- | --- | ---: |
| Ordinary ready work | 200 sequential publish → claim → acknowledge workflows | 702.635 ms |
| Recovery | 50 publish → claim → retry(0) → claim → fail → page → redrive(0) → claim → acknowledge → find workflows | 429.332 ms |
| Delayed seeding | 10,000 individually committed publications delayed by one hour | 9823.905 ms |
| Ready work with backlog | 200 ready records drained while 10,000 future records remained | 391.201 ms |
| Schema setup | Connections and queue schema installation | 22.424 ms |
| Entire measured invocation | Setup, warmup, all scenarios, query plans and cleanup | 11751.563 ms |

Every publish, claim, settlement, retry and redrive in the timed workload used
the public API and its normal transaction path. The runner did not seed messages
with direct SQL. It verified message identity/body and retry/delivery counters,
checked redriven failure history, and confirmed that all **10,000 future records
remained ineligible** after draining ready work and making 20 empty polls.

The delayed queue also had one extra ready record for the untimed plan-capture
probe. It was acquired and acknowledged before measuring the 200-record drain.
`ANALYZE` ran after population and before plan capture/drain. The benchmark did
not force a planner setting or execute `VACUUM`.

Explicit adapter settings were: 30,000 ms lease; five maximum deliveries;
3,600,000 ms deduplication and failure-history retention; 5,000 ms I/O timeout;
maintenance limit 10; polling interval 10 ms. Receives were nonwaiting calls.
Retries and redrives used zero delay; positive-delay *retry latency* was not
measured.

## Latency distributions

All values below are **milliseconds**. Each API timer uses monotonic `hrtime` and
ends only when the public call returns. The `workflow` rows include their nested
API calls and local verification, so their times must not be added again to the
individual-operation rows. Percentiles use nearest rank: sorted sample at
`ceil(p × n)`. Consequently, with 50 recovery samples, p99 is the observed maximum;
these small tail samples are not a stable tail-latency estimate.

| Timed operation | Samples | p50 | p95 | p99 | Maximum |
| --- | ---: | ---: | ---: | ---: | ---: |
| `ready.publish` | 200 | 1.053 | 1.974 | 4.993 | 28.999 |
| `ready.claim` | 200 | 1.115 | 2.078 | 3.279 | 5.770 |
| `ready.acknowledge` | 200 | 0.905 | 1.326 | 1.722 | 6.642 |
| `ready.workflow` | 200 | 3.160 | 5.182 | 8.809 | 33.607 |
| `recovery.publish` | 50 | 0.891 | 1.272 | 1.541 | 1.541 |
| `recovery.claim_initial` | 50 | 0.940 | 1.196 | 2.012 | 2.012 |
| `recovery.retry` | 50 | 0.811 | 1.263 | 1.681 | 1.681 |
| `recovery.claim_retry` | 50 | 0.953 | 1.209 | 1.421 | 1.421 |
| `recovery.fail` | 50 | 0.932 | 1.240 | 2.630 | 2.630 |
| `recovery.failure_page` | 50 | 0.264 | 0.402 | 1.559 | 1.559 |
| `recovery.redrive` | 50 | 1.101 | 1.579 | 2.002 | 2.002 |
| `recovery.claim_redrive` | 50 | 0.971 | 1.330 | 1.687 | 1.687 |
| `recovery.acknowledge` | 50 | 0.797 | 1.129 | 2.826 | 2.826 |
| `recovery.failure_find` | 50 | 0.284 | 0.498 | 0.727 | 0.727 |
| `recovery.workflow` | 50 | 8.463 | 9.869 | 13.504 | 13.504 |
| `backlog.delayed_publish` | 10,000 | 0.869 | 1.293 | 2.822 | 50.678 |
| `backlog.ready_publish` | 200 | 0.902 | 1.178 | 1.512 | 2.650 |
| `backlog.claim` | 200 | 1.053 | 1.327 | 2.505 | 3.562 |
| `backlog.acknowledge` | 200 | 0.787 | 1.008 | 1.072 | 2.472 |
| `backlog.drain_workflow` | 200 | 1.893 | 2.246 | 3.629 | 4.493 |
| `backlog.idle_poll` | 20 | 0.346 | 0.428 | 0.501 | 0.501 |

The baseline and backlog values describe two sequential scenarios on the same
shared host. Their difference is not a controlled estimate of the backlog's
causal cost: data/state, cache contents, checkpoint activity and host contention
can differ. No optimization speedup is inferred from this run.

## Query-plan evidence

The runner captured the actual ready/expired candidate `SELECT` statements from
a separate queue connection. It executed `EXPLAIN ANALYZE` inside short
transactions and rolled those transactions back, releasing probe locks.

| Candidate | Observed path | Actual returned rows | Shared hits at root | Shared reads | Execution time |
| --- | --- | ---: | ---: | ---: | ---: |
| Ready head | `Limit → LockRows → Index Scan queue_ready` | 1 | 170 | 0 | 0.229 ms |
| Expired head | `Limit → LockRows → Index Scan queue_leased` | 0 | 2 | 0 | 0.017 ms |

Both plans constrained the configured queue and the relevant timestamp in the
index condition. Neither contained a sort or sequential scan. The ready index
scan itself reported 169 shared hits for the one returned row, so the evidence
does **not** establish a one-page or constant-I/O claim. The captured plans were
warm-cache observations with zero physical reads. The expired-head probe had no
expired lease to return; throughput with an expired-delivery backlog was not
measured.

The 169 ready-index hits were reported to the adapter author for follow-up. One
possible contributor is obsolete index entries from inserting a publication and
then anchoring its availability under the same transaction; this is an
interpretation, not a cause established by this experiment. A controlled
comparison would need repeated equivalent datasets and a documented vacuum/cache
state before changing implementation for performance.

## Counts and resources

The invocation completed **11,783 counted public API calls**, including
warmup and the untimed plan probe. `claim` entries are `receive()` calls that
returned a delivery; the 20 idle calls returned `null`. This is an API-operation
count, not a count of SQL statements or storage flushes.

| Public operation | Count |
| --- | ---: |
| `warmup.publish` | 20 |
| `warmup.receive` | 20 |
| `warmup.acknowledge` | 20 |
| `ready.publish` | 200 |
| `ready.claim` | 200 |
| `ready.acknowledge` | 200 |
| `recovery.publish` | 50 |
| `recovery.claim_initial` | 50 |
| `recovery.retry` | 50 |
| `recovery.claim_retry` | 50 |
| `recovery.fail` | 50 |
| `recovery.failure_page` | 50 |
| `recovery.redrive` | 50 |
| `recovery.claim_redrive` | 50 |
| `recovery.acknowledge` | 50 |
| `recovery.failure_find` | 50 |
| `backlog.delayed_publish` | 10,000 |
| `plan_probe.publish` | 1 |
| `backlog.ready_publish` | 200 |
| `plan_probe.receive` | 1 |
| `plan_probe.acknowledge` | 1 |
| `backlog.claim` | 200 |
| `backlog.acknowledge` | 200 |
| `backlog.idle_poll` | 20 |

The PHP process reported 8,388,608 peak allocated bytes,
32,512 KiB maximum RSS, 1.284457 seconds
of user CPU and 1.210221 seconds of system CPU across the
invocation. These are PHP-process observations; PostgreSQL CPU/RSS was not
instrumented. The host and storage were shared and were not isolated for
performance experiments. Enabled durability settings are reported as observed;
underlying hardware durability was not independently measured.

## Reproduce

Install the root Composer development dependencies and prepare an isolated
PostgreSQL 16+ database whose user can create a schema. Run from the repository
root, supplying a database dedicated to this benchmark:

```sh
QUEUE_BENCH_DSN='pgsql:host=127.0.0.1;port=5432;dbname=queue_bench;connect_timeout=2' \
QUEUE_BENCH_USER=queue_bench \
php benchmarks/QueueWorkload.php \
  --messages=200 --recovery-messages=50 --delayed-backlog=10000 \
  --backlog-ready=200 --warmup=20 --payload-bytes=256 > postgres-baseline.json
```

If authentication requires a password, supply `QUEUE_BENCH_PASSWORD` through the
invoking environment. Credentials are not written to the result. `--help` shows
the supported workload options; malformed/out-of-range options and a missing
explicit DSN fail before database access. A successful JSON result is emitted
only after cleanup succeeds.

A small smoke run and an interim full run preceded the final source-stable run
reported above. Their measurements are not pooled because the source changed.
There are **no concurrent-consumer measurements**, no cold-cache trials, no
repeated-run confidence intervals, no remote-network measurements and no business
handler costs here. Correctness under concurrency and commit uncertainty belongs
to the separate integration suite; this baseline does not replace those gates.

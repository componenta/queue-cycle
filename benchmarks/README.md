# PostgreSQL workloads

These scripts measure actual synchronous PostgreSQL operations through the public
queue API. Use an isolated benchmark database whose role can create and drop
schemas. Each run creates a random schema, preserves other schemas, and emits
successful JSON results only after its own cleanup completes.

```sh
export QUEUE_BENCH_DSN='pgsql:host=127.0.0.1;port=5432;dbname=queue_bench;connect_timeout=5'
export QUEUE_BENCH_USER=queue_bench
export QUEUE_BENCH_PASSWORD=queue_bench
php benchmarks/QueueWorkload.php > queue-workload.json
php benchmarks/CleanupWorkload.php --sizes=100,1000,10000 --samples=20 > cleanup-workload.json
```

Both scripts require an explicit benchmark DSN with a finite connection timeout;
they do not inherit the test DSN or choose an application database implicitly.
Run `--help` for supported options. Record the actual PHP/PostgreSQL/Cycle versions,
durability settings and source fingerprints in the emitted JSON when comparing
runs. Run comparisons without concurrent benchmark or test traffic.

## Publication, processing and recovery

`QueueWorkload.php` measures publication, reservation, acknowledgment, retry,
failure capture, failure pagination and redrive. It also processes ready work
beside a future backlog and captures the actual claim queries with
`EXPLAIN (ANALYZE, BUFFERS)` outside the timed calls.

## Idle cleanup scaling

`CleanupWorkload.php` publishes future work through the public publisher, then
calls `cleanup(1)` at each configured backlog size. Three untimed warmup calls
precede the measured calls. Every call must report zero effects, and the script
checks that all future messages remain. Publication and `ANALYZE` are outside the
cleanup latency samples.

The script reports mean, p50 and p95 in milliseconds using a monotonic clock and
nearest-rank percentiles. The query-plan probe captures the actual candidate
selection query and executes it in a separate rolled-back transaction. No query
text or planner shape is asserted in the behavior test suite.
The Cycle SQL logger remains installed during timing; disabling trace capture
does not disable query interpolation. Absolute latency therefore includes this
instrumentation cost. Compare runs using the same instrumentation.

This scenario detects cleanup that scans live/future work merely to establish
that nothing is due. An independent indexed existence check skips that empty
case. When work is due, the candidate query retains its ordered scan and stops
after enough unlocked rows; it does not enumerate and deduplicate the entire
due set first. Sparse due records after many live/future rows can still require
a long scan, so the operation budget does not promise constant query cost.
The two scripts use one caller and do not establish multi-consumer capacity,
remote-network latency, replication failover behavior or business-effect speed.

# Verification evidence

Date: 2026-11-27. The source was extracted from
[componenta/queue commit a90692f](https://github.com/componenta/queue/commit/a90692fe8d62ffba9baa2002555e8ffdea413cf3)
into its own repository without changing the transport implementation. Composer
bootstrap, development installation, CI and workload source discovery now use
the standalone package layout.

## Standalone execution

| Check | Observed result |
| --- | --- |
| Independent Composer installation | Production manifest validates; development root resolves real core 1.0.x-dev and Cycle Database 2.24.0 |
| Pest / real PostgreSQL 17.12 | 56 passed, 262 assertions; 15.90 seconds |
| Shared portable adapter contract | All 11 scenarios passed using the installed core package |
| PHPStan maximum level | Zero errors, no baseline or suppression |
| PSR-12 formatting | Zero pending changes in 33 PHP files |

The test environment used PHP 8.5.13, Pest 4.7.8 and PHPUnit 12.5.33. Its isolated
database server was stopped after completion. The same suite previously passed
on PostgreSQL 16.16, 17.12 and 18.7 in
[core CI run 46007356923](https://github.com/componenta/queue/actions/runs/46007356923).
The standalone [Actions run 46010762517](https://github.com/componenta/queue-cycle/actions/runs/46010762517)
passed for commit `b902fb770c0814883c87069f61e2de121fa7a448`. Its actual logs
confirmed PostgreSQL **16.16, 17.12 and 18.7**; every job passed **56 tests /
262 assertions**, strict Composer validation, maximum-level PHPStan and PSR-12
checks. This confirmation applies to the independent package and its separate
Composer installation.

## Follow-up audit

The complete standalone suite passed **60 tests / 281 assertions** on PostgreSQL
17.12 after two additional defects were reproduced and fixed. PHPStan maximum
level and PSR-12 checks also passed.

- `ConfigurationTest.php` first reproduced failed timestamp reads under
  `DateStyle = 'SQL, DMY'`. It now verifies microsecond-preserving delivery
  metadata, renewal, acknowledgment, restoration after commit and rollback, and
  unchanged settings during caller-owned staging. ISO/YMD is selected locally
  in the existing transaction-settings query, without an extra round trip.
- `PostgresQueueTest.php` first reproduced NUL parameter truncation in find,
  redrive and discard. An ID containing NUL cannot exist in PostgreSQL text and
  now follows the not-found contract. Negative controls preserve the original
  pending record; positive controls still redrive or discard its exact ID.

These checks exercise public ports against real PostgreSQL. The earlier 56-test
CI and workload records above and below remain historical measurements of their
identified source snapshots.

## Capability-copy audit

The next public PostgreSQL probe reproduced a cloned delivery acknowledging its
reservation after the original receiver had closed. Its copied mutable state
was detached from the session's completion callback. Receiver and delivery
objects now reject cloning so a partially copied resource cannot acquire a
second local lifecycle.

The two new ownership cases first failed because cloning succeeded. After the
fix they both reject cloning, retain successful acknowledgment through the
original delivery, and reject renewal of its completed lease. The complete suite
passed **62 tests / 287 assertions** on PostgreSQL 17.12, with maximum-level
PHPStan and PSR-12 checks passing. This changes resource ownership behavior, not
the SQL protocol; earlier workload measurements retain their original scope.

## Behavioral evidence

Tests exercise public publisher, receiver, delivery, lease and administration
ports. Independent connections and subprocesses verify SKIP LOCKED claims,
strict post-lock expiry checks, stale-token rejection, crashed-worker recovery,
publication deduplication and concurrent redrive/discard/cleanup.

Real database constraint failures prove atomic rollback of reservation, retry,
failure capture and redrive transitions. A TCP relay loses COMMIT either before
the server receives it or after the server completes it; a separate connection
inspects durable state. An uncertain claim never invokes a handler, and an
uncertain acknowledgment never triggers an opposite disposition.

Staging tests preserve caller transaction ownership and settings, hide uncommitted
work from another connection, verify rollback, and anchor delay before a long
business transaction commits. Replica isolation is tested with a distinct empty
READ schema while all authoritative operations continue on WRITE.

Exact same-microsecond lease equality is covered by the core's frozen-clock
memory suite. The live database checks real expiry and blocking, without claiming
to force a same-microsecond coincidence. Unsupported server versions, unusually
small-block PostgreSQL builds and multi-consumer throughput are not measured.
The separate [workload baseline](evidence/postgres-baseline.md) records actual
commits, a 10,000-message delayed backlog and query plans.

## Standalone workload rerun

The relocated workload was executed from this independent package using its own
Composer installation. The [raw result](../benchmarks/results/postgres-standalone-2026-11-27.json)
contains all timings and actual query plans. With fsync, synchronous_commit and
full_page_writes enabled, it performed 11,783 public operations in 11.510 seconds,
including setup, 10,000 delayed publications and cleanup.

| Scenario | p50 | p95 | p99 |
| --- | --- | --- | --- |
| Publish / claim / acknowledge workflow | 2.652 ms | 3.525 ms | 4.201 ms |
| Claim with 10,000 delayed messages present | 0.910 ms | 1.527 ms | 2.920 ms |

All 10,000 future messages remained ineligible. The actual ready/expired plans
used the corresponding indexes. Schema cleanup was confirmed and the owned
PostgreSQL server was stopped. Before/after source fingerprints matched
`e3bccfed18966d453f7bd9834d3f35d1c0e3d14e744fecca318c12cb63639a58`. These are one-consumer observations on
a shared host; different timings from the historical run do not establish a
performance improvement or a production capacity guarantee.

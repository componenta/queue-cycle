# Componenta Queue — Cycle Database adapter

This package implements a durable PostgreSQL queue through Cycle Database. It
provides the publisher, managed receiver, failed-message store and cooperative
lease interfaces defined by `componenta/queue`. Publication, claim, retry,
acknowledgment, failure capture and redrive use short transactions on the
**physical write driver** of the supplied `DatabaseInterface`.

[Русский обзор](README.ru.md)

The adapter has its own Composer manifest and test suite. Its only Componenta
runtime dependency is [componenta/queue](https://github.com/componenta/queue).
The [normative storage profile](https://github.com/componenta/queue/blob/main/docs/cycle-profile.md)
defines ownership, durability and recovery semantics.

## Requirements

- PHP **8.5.13+ within 8.x**, on a **64-bit** platform, with `pdo_pgsql`.
- `cycle/database:^2.24` and `componenta/queue:^1.0`.
- PostgreSQL **16+**, server and client encoding `UTF8`, block size at least
  8192 bytes, and a dedicated queue schema created by the application operator.
- A writable, correctly configured Cycle PostgreSQL driver. Keep reconnect
  disabled for the queue connection. All ordinary queue operations require its
  transaction nesting level to be zero.
- Committed database durability appropriate to the deployment. The integration
  evidence uses `fsync=on` and `synchronous_commit=on`. Replication failover and
  external side-effect durability remain deployment responsibilities.

The adapter was verified on PostgreSQL 16.16, 17.12 and 18.7 before extraction
from the core repository. The standalone package reruns that matrix and the same
portable transport contract; see [verification evidence](docs/verification.md).
MySQL, SQLite and other Cycle drivers are not supported by this package.

## Construct the adapter

Supply an existing Cycle database object. Use a dedicated write connection for
ordinary queue operations when the application also has long business
transactions. `DatabaseInterface::query()` can select a read driver; this adapter
uses `getDriver(DatabaseInterface::WRITE)` for every authoritative operation.

```php
use Componenta\Queue\Codec\JsonMessageCodec;
use Componenta\Queue\Cycle\PostgresQueue;
use Componenta\Queue\Cycle\QueueConfig;
use Componenta\Queue\Cycle\Schema;
use Componenta\Queue\MessageLimits;

// $database is an application-configured Cycle\Database\DatabaseInterface.
// Run installation once from an explicit migration/admin step.
Schema::install($database);

$codec = new JsonMessageCodec(new MessageLimits(
    maxBodyBytes: 1_000_000,
    maxWireBytes: 2_000_000,
    maxHeaderBytes: 4_096,
    maxJsonDepth: 16,
));
$queue = new PostgresQueue($database, new QueueConfig(
    queue: 'orders',
    leaseMilliseconds: 30_000,
    maxDeliveries: 10,
    dedupRetentionMilliseconds: 7 * 24 * 60 * 60 * 1_000,
    failureRetentionMilliseconds: 30 * 24 * 60 * 60 * 1_000,
    ioTimeoutMilliseconds: 5_000,
    maintenanceLimit: 25,
    pollIntervalMilliseconds: 100,
), $codec);

$publisher = $queue->publisher();
$receiver = $queue->receiver();
$failures = $queue->failures();
```

Every configuration choice above is an application decision, not an implicit
retention or delivery limit. The public duration range is `0` through
`QueueConfig::MAX_DURATION_MILLISECONDS` (9,007,199,254,740 ms); leases and polling
intervals require a positive duration. Values outside the supported range fail
before a queue change. Queue names contain 1–128 UTF-8 bytes without control
characters. Logical message IDs contain at most 255 UTF-8 bytes in this profile.
Identity comparisons use exact bytes under PostgreSQL's `C` collation; no case
folding or Unicode normalization occurs.

`Schema::install()` is intentionally explicit and fails if these tables already
exist. It does not silently migrate or replace an installed store. The template
in `resources/schema.sql` defines two tables and their indexes. Installation also
sets the adapter-owned table comment
`componenta.queue.failure-store.v1:<installation-id>` on `queue_failures`.
Preserve this marker when migrating the same store. A fresh installation receives
a fresh marker so old pagination cursors cannot address replacement history.
The marker is schema metadata; do not overwrite it with an application comment.

## Publish and process one message

```php
use Componenta\Queue\Handler\CallbackHandler;
use Componenta\Queue\Message;
use Componenta\Queue\MessageType;
use Componenta\Queue\Retry\NoRetryPolicy;
use Componenta\Queue\Worker;

$publisher->publish(new Message(
    id: 'order-482-confirmation-v1',
    type: new MessageType('orders.confirmation', 1),
    body: '{"orderId":482}',
));

$worker = new Worker($receiver, $codec, new CallbackHandler(
    function (Message $message, $context): void {
        // Perform the application operation. Acknowledge follows successful return.
        // For bounded long-running steps, $context->lease?->renew(30_000).
    },
), new NoRetryPolicy());

$result = $worker->processOne(waitMilliseconds: 1_000);
```

The application owns the worker loop, shutdown and process supervision. A
receiver permits one outstanding delivery. `close()` invalidates its local
capabilities, prevents another receive, and leaves active work to expire. It does
not acknowledge that work or disconnect the borrowed application driver. Normal
shutdown should finish the in-flight handler before closing the receiver.

Raw bytes and string headers are acquired before codec decoding. A malformed
envelope can therefore be captured in the failed-message store without invoking
a handler. The raw original is preserved through retry, failure and redrive.

## Ownership, recovery and counters

A claim commits a new random reservation token, DB-time lease expiry and exact
reservation counts. Only then does the receiver return a delivery. Acknowledge,
retry, fail and renew lock that ledger row first, then sample
`clock_timestamp()`. They require the same queue, row, token and leased state,
with `lease_until` strictly greater than that fresh time. An expired token cannot
be revived even when no other consumer has reclaimed the record.

Renewal sets a new expiry from current DB time; it does not add a duration to the
previous expiry. It is cooperative, with no background thread or signal-driven
SQL. Handlers execute outside database transactions. A lease does not fence
external business effects: use application idempotency or a transactional inbox
where that guarantee is needed.

`retryCount` counts durable explicit retry transitions in the current processing
generation. `deliveryCount` counts every committed reservation in that generation,
including reservations whose PHP process dies before handling. Redrive resets
both generation counts and keeps lifetime delivery/redrive counters in storage.

Once a ready or expired record has exhausted its positive `maxDeliveries`, a
later receive pass atomically moves it to pending failure with reason
`delivery_limit`. Each transaction handles at most `maintenanceLimit` candidates.
A null receive can mean that the bounded pass performed this maintenance; it is
not a proof that the entire destination is empty. Separate ordered partial-index
queries select ready and expired heads, and the older eligible head is considered
first. Strict FIFO is not promised.

## Publication identity and retention

The unique identity is `(queue, logical message ID)`. The first accepted
publication stores its exact raw body, canonical string-header map and original
requested publication delay. A repeat with identical content is a no-op while
that ledger incarnation remains. Changing body, raw headers or original delay
raises `MessageConflictException`. Header insertion order is irrelevant; names
and values remain byte-sensitive. Deterministic codec output is required for
repeatable publication identity.

Completed and explicitly discarded records keep their original bytes as dedup
tombstones. `dedupRetentionMilliseconds` starts at that terminal transition;
`null` means indefinite retention. Active work and unresolved failures never
enter implicit expiry cleanup. Choose a finite period only after bounding the
latest possible producer resend, including uncertain-publication recovery.
After explicit cleanup removes a tombstone, the same logical ID can create a
new incarnation and execute again. Expiry permits cleanup; it does not make an
existing retained row disappear during publication.

## Failed work and bounded cleanup

```php
use Componenta\Queue\FailureQuery;
use Componenta\Queue\FailureStatus;

$page = $failures->page(new FailureQuery(limit: 50, status: FailureStatus::Pending));
foreach ($page->items as $summary) {
    $snapshot = $failures->find($summary->failureId);
    // Review first, then explicitly choose redrive($id, $delay) or discard($id).
}
$cleanup = $queue->cleanup(limit: 100);
```

Failure snapshot insertion and removal of the source reservation commit in one
transaction. Failure pages contain summaries, not bodies, and use opaque cursors
bound to the destination, installation and status filter. `find()` retrieves the
raw snapshot and diagnostic details. There is no automatic re-enqueue operation.

Failure IDs are matched exactly. An ID containing NUL cannot exist in this
PostgreSQL profile: `find()` returns `null`, while `redrive()` and `discard()`
throw `FailureNotFoundException`. It never addresses a truncated prefix.

Redrive creates one new processing generation in the same ledger incarnation,
with the same logical ID and raw original. Repeating the same failure ID and delay
is idempotent while that history record remains. Different delay parameters or
redrive after discard conflict. Discard after redrive also conflicts, so it cannot
cancel the later run. Resolved history retention starts at redrive/discard;
`failureRetentionMilliseconds: null` retains that decision indefinitely. Once
explicit cleanup removes resolved history, its failure ID is not found and its
administrative idempotency window has ended.

`cleanup($limit)` bounds deletions and history detachments together by the supplied
operation budget. It locks selected ledger rows before touching their histories.
Expired resolved history can be removed while a newer generation remains active.
For an expiring terminal ledger, unexpired resolved history is detached before
deleting the ledger; retained old failure IDs never rebind to a later publication.
A small budget can detach history in one call and delete the ledger in a later
call. Unresolved failed records are preserved. Schedule cleanup explicitly and
call it repeatedly when a backlog exists; competing locks can defer work.

## Business transactions and the staging extension

Ordinary `publish()`, receive, settlement and failure administration reject an
already-open transaction on their write driver. A nested Cycle transaction or
savepoint cannot turn application-owned work into a durable queue commit.

`$queue->transactionalPublisher()->stage($message, $delay)` is the explicit
extension for a local transactional outbox: begin a business transaction on the
**same physical write driver**, change business rows and stage the publication,
then let the application commit or roll back. Staging never changes the caller's
timeouts/isolation, commits, or rolls back their transaction. It is invisible to
other connections until the business commit and disappears on rollback. Its
delay starts when staged, so a long transaction can consume that delay before
the message becomes visible. SQL staging errors leave transaction resolution
with the caller; do not interpret a failed stage call as a confirmed rollback.

This provides atomic local business changes plus queue acceptance. It does not
make arbitrary remote handler effects exactly once, and two separately configured
connections to the same database do not share a transaction.

## I/O limits and uncertain outcomes

Adapter-owned transactions select `DateStyle = 'ISO, YMD'` locally so PostgreSQL
timestamp results remain unambiguous and preserve microseconds. Commit or rollback
restores the borrowed session's original setting. Transactional staging keeps the
caller's DateStyle unchanged and uses explicitly formatted timestamp expressions.

`ioTimeoutMilliseconds` applies PostgreSQL `statement_timeout` and `lock_timeout`
inside adapter-owned transactions only. These bound server-side statements and
lock waits. They do **not** by themselves bound connecting, a blackholed TCP read,
DNS resolution or the complete wall-clock duration of a queue call. Configure
appropriate libpq connection/network limits and process-level supervision for
the deployment. A finite receive wait controls polling, not cancellation of an
already-issued SQL operation. Staging uses the caller's existing limits.

A confirmed rollback is reported as `NotApplied`. If COMMIT was attempted and
its result cannot be confirmed, or rollback cannot be confirmed, the outcome is
`Indeterminate`. The adapter does not issue an opposite disposition or assume
reconnection reveals the previous result. An uncertain or lost delivery closes
its local receiver session. Discard that unusable connection/session and recover
with fresh ones; redelivery after expiry can repeat handler effects.

An uncertain publication can be retried explicitly with the same message ID,
bytes, headers and original delay while the dedup identity remains retained.
Never regenerate the ID to infer that the first publication failed.

## Testing and evidence

For a stable dependency installation, use `composer install`. To test together
with a core development checkout, generate an explicit development Composer root:

```sh
php tools/development.php ../componenta-queue
COMPOSER=composer.dev.json composer update
```

This keeps production `componenta/queue:^1.0` intact and permits only its actual
1.0 development line in the local fixture. CI pins the reviewed core commit.
Set an explicit **isolated test database**; the test fixture creates and drops
only its random per-test schemas.

```sh
QUEUE_TEST_DSN='pgsql:host=127.0.0.1;port=5432;dbname=queue_tests;connect_timeout=2' \
QUEUE_TEST_USER=queue_test \
COMPOSER=composer.dev.json composer test
```

`QUEUE_TEST_PASSWORD` is optional. The test role needs schema/table creation and
drop privileges in that test database; the suite must not target production.
Independent-process tests need `proc_open`, and the lost-COMMIT relay needs local
TCP sockets. No SQLite database or mocked PDO substitutes for these tests.
The bootstrap maps the portable contract suite shipped by the installed core
package. It has no fallback to another checkout or another project's vendor tree.

The suite shares the portable transport contract with the core in-memory
adapter, then adds real transaction, lock, expiry, process-death and fault
experiments. The wire relay can discard COMMIT before forwarding it or forward
it and suppress the response after PostgreSQL completes it. Tests inspect durable
state using an independent connection and prove that an uncertain claim never
invokes a handler. The performance workload under `benchmarks/` separately
measures actual commits and explains the ready/expired queries against a delayed
backlog. See [verification evidence](docs/verification.md) and the
[workload baseline](docs/evidence/postgres-baseline.md) for executed versions and observations; this package does not assert production
throughput or exactly-once effects from a test count.

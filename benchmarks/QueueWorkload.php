<?php

declare(strict_types=1);

/**
 * Exploratory, single-consumer PostgreSQL workload; run from the repository root.
 *
 * QUEUE_BENCH_DSN='pgsql:host=127.0.0.1;port=5432;dbname=queue_bench;connect_timeout=2' \
 * QUEUE_BENCH_USER=queue_bench php benchmarks/QueueWorkload.php
 *
 * The explicitly selected database must allow CREATE SCHEMA. Only a newly
 * generated schema is used, and it is dropped in finally. No test DSN fallback
 * exists. The script emits JSON after successful cleanup, never credentials.
 */

namespace Componenta\Queue\Cycle\Benchmarks;

use Closure;
use Componenta\Queue\Codec\JsonMessageCodec;
use Componenta\Queue\Cycle\PostgresQueue;
use Componenta\Queue\Cycle\QueueConfig;
use Componenta\Queue\Cycle\Schema;
use Componenta\Queue\DeliveryInterface;
use Componenta\Queue\FailureDetails;
use Componenta\Queue\FailureQuery;
use Componenta\Queue\FailureReason;
use Componenta\Queue\FailureStatus;
use Componenta\Queue\Headers;
use Componenta\Queue\Message;
use Componenta\Queue\MessageLimits;
use Componenta\Queue\MessageType;
use Componenta\Queue\ReceiverInterface;
use Composer\InstalledVersions;
use Cycle\Database\Config\Postgres\DsnConnectionConfig;
use Cycle\Database\Config\PostgresDriverConfig;
use Cycle\Database\Database;
use Cycle\Database\Driver\DriverInterface;
use Cycle\Database\Driver\Postgres\PostgresDriver;
use InvalidArgumentException;
use Psr\Log\AbstractLogger;
use RuntimeException;
use Stringable;
use Throwable;

$root = dirname(__DIR__);
require $root . '/vendor/autoload.php';

/** Captures actual claim SELECTs on a separate, untimed connection. */
final class ClaimTrace extends AbstractLogger
{
    /** @var list<string> */
    public array $queries = [];

    public function log($level, string|Stringable $message, array $context = []): void
    {
        $sql = (string) $message;
        if (str_starts_with(ltrim($sql), 'SELECT')
            && str_contains($sql, 'queue_messages')
            && str_contains($sql, 'FOR UPDATE SKIP LOCKED')) {
            $this->queries[] = $sql;
        }
    }
}

final class Workload
{
    /** @var array<string, list<float>> */
    private array $samples = [];

    /** @var array<string, int> */
    private array $operationCounts = [];

    /** @var list<ReceiverInterface> */
    private array $receivers = [];

    /** @var list<DriverInterface> */
    private array $drivers = [];

    private readonly JsonMessageCodec $codec;
    private readonly string $body;
    private readonly string $schema;

    /** @param array<string, int> $options */
    public function __construct(
        private readonly string $dsn,
        private readonly ?string $user,
        private readonly ?string $password,
        private readonly array $options,
        private readonly string $root,
    ) {
        $this->schema = 'queue_bench_' . bin2hex(random_bytes(12));
        $this->body = str_repeat('x', $options['payload-bytes']);
        $this->codec = new JsonMessageCodec(new MessageLimits(1_048_576, 2_097_152, 4_096, 8));
    }

    /** @return array<string, mixed> */
    public function run(): array
    {
        $sourceBefore = $this->sourceFingerprint();
        $admin = $this->driver('public');
        $schemaCreated = false;
        $failure = null;
        $result = [];
        $start = hrtime(true);
        $usageBefore = getrusage();

        try {
            $admin->execute('CREATE SCHEMA "' . $this->schema . '"');
            $schemaCreated = true;
            $driver = $this->driver($this->schema);
            $database = new Database('queue-benchmark', '', $driver);
            Schema::install($database);
            $environment = $this->row($driver, "SELECT version() AS version,
                current_setting('server_version') AS server_version,
                current_setting('server_encoding') AS server_encoding,
                current_setting('client_encoding') AS client_encoding,
                current_setting('fsync') AS fsync,
                current_setting('synchronous_commit') AS synchronous_commit,
                current_setting('full_page_writes') AS full_page_writes,
                current_setting('wal_sync_method') AS wal_sync_method,
                current_setting('default_transaction_isolation') AS default_transaction_isolation,
                current_setting('block_size') AS block_size,
                current_setting('shared_buffers') AS shared_buffers");
            $setupMilliseconds = (hrtime(true) - $start) / 1_000_000;

            $this->warmup($this->queue($database, 'warmup'));
            $ready = $this->ready($this->queue($database, 'ready'));
            $recovery = $this->recovery($this->queue($database, 'recovery'));
            $backlog = $this->backlog($database, $driver);

            $result = [
                'method' => 'single consumer; sequential synchronous calls; monotonic hrtime; nearest-rank percentiles',
                'measured_at_utc' => gmdate('Y-m-d\TH:i:s\Z'),
                'php_version' => PHP_VERSION,
                'cycle_database_version' => InstalledVersions::getPrettyVersion('cycle/database'),
                'os' => php_uname('s') . ' ' . php_uname('r') . ' ' . php_uname('m'),
                'database' => $environment,
                'configuration' => $this->options + [
                    'delayed_milliseconds' => 3_600_000,
                    'lease_milliseconds' => 30_000,
                    'max_deliveries' => 5,
                    'dedup_retention_milliseconds' => 3_600_000,
                    'failure_retention_milliseconds' => 3_600_000,
                    'io_timeout_milliseconds' => 5_000,
                    'maintenance_limit' => 10,
                    'poll_interval_milliseconds' => 10,
                    'consumers' => 1,
                ],
                'schema_setup_elapsed_ms' => $setupMilliseconds,
                'scenarios' => ['ready' => $ready, 'recovery' => $recovery, 'delayed_backlog' => $backlog],
                'latency_ms' => array_map($this->statistics(...), $this->samples),
                'api_operation_counts' => $this->operationCounts,
                'peak_php_allocated_bytes' => memory_get_peak_usage(true),
                'source_sha256_before' => $sourceBefore,
                'source_sha256_after' => $this->sourceFingerprint(),
                'benchmark_sha256' => hash_file('sha256', __FILE__),
                'multi_consumer_measurement' => false,
                'business_effects_measured' => false,
            ];
        } catch (Throwable $error) {
            $failure = $error;
        } finally {
            $cleanupFailures = [];
            foreach ($this->receivers as $receiver) {
                try {
                    $receiver->close();
                } catch (Throwable $error) {
                    $cleanupFailures[] = $error;
                }
            }
            foreach ($this->drivers as $owned) {
                if ($owned === $admin) {
                    continue;
                }
                try {
                    if ($owned->getTransactionLevel() > 0 && !$owned->rollbackTransaction()) {
                        throw new RuntimeException('An owned benchmark connection did not confirm rollback.');
                    }
                } catch (Throwable $error) {
                    $cleanupFailures[] = $error;
                } finally {
                    try {
                        $owned->disconnect();
                    } catch (Throwable $error) {
                        $cleanupFailures[] = $error;
                    }
                }
            }
            try {
                if ($schemaCreated) {
                    $admin->execute('DROP SCHEMA "' . $this->schema . '" CASCADE');
                }
            } catch (Throwable $error) {
                $cleanupFailures[] = $error;
            } finally {
                try {
                    $admin->disconnect();
                } catch (Throwable $error) {
                    $cleanupFailures[] = $error;
                }
            }
            if ($cleanupFailures !== []) {
                fwrite(STDERR, 'Cleanup did not finish cleanly; inspect generated schema ' . $this->schema . ".\n");
                $failure ??= $cleanupFailures[0];
            }
        }

        if ($failure !== null) {
            throw $failure;
        }

        $usageAfter = getrusage();
        $result['total_elapsed_including_setup_cleanup_ms'] = (hrtime(true) - $start) / 1_000_000;
        $result['php_cpu_user_seconds'] = self::cpuSeconds($usageAfter, 'utime') - self::cpuSeconds($usageBefore, 'utime');
        $result['php_cpu_system_seconds'] = self::cpuSeconds($usageAfter, 'stime') - self::cpuSeconds($usageBefore, 'stime');
        $result['process_max_rss_kib_linux'] = PHP_OS_FAMILY === 'Linux' ? $usageAfter['ru_maxrss'] : null;
        $result['schema_cleanup_confirmed'] = true;

        return $result;
    }

    private function warmup(PostgresQueue $queue): void
    {
        $publisher = $queue->publisher();
        $receiver = $this->receiver($queue);
        for ($i = 0; $i < $this->options['warmup']; ++$i) {
            $publisher->publish($this->message('warmup-' . $i));
            $this->delivery($receiver)->acknowledge();
        }
        foreach (['publish', 'receive', 'acknowledge'] as $operation) {
            $this->operationCounts['warmup.' . $operation] = $this->options['warmup'];
        }
    }

    /** @return array<string, int|float> */
    private function ready(PostgresQueue $queue): array
    {
        $publisher = $queue->publisher();
        $receiver = $this->receiver($queue);
        $start = hrtime(true);
        for ($i = 0; $i < $this->options['messages']; ++$i) {
            $message = $this->message('ready-' . $i);
            $this->measure('ready.workflow', function () use ($publisher, $receiver, $message): void {
                $this->measure('ready.publish', fn () => $publisher->publish($message), true);
                $delivery = $this->measure('ready.claim', fn () => $this->delivery($receiver), true);
                $this->verify($delivery, $message->id, 0, 1);
                $this->measure('ready.acknowledge', $delivery->acknowledge(...), true);
            });
        }

        return ['messages' => $this->options['messages'], 'elapsed_ms' => (hrtime(true) - $start) / 1_000_000];
    }

    /** @return array<string, int|float> */
    private function recovery(PostgresQueue $queue): array
    {
        $publisher = $queue->publisher();
        $receiver = $this->receiver($queue);
        $failures = $queue->failures();
        $start = hrtime(true);
        for ($i = 0; $i < $this->options['recovery-messages']; ++$i) {
            $message = $this->message('recovery-' . $i);
            $this->measure('recovery.workflow', function () use ($publisher, $receiver, $failures, $message): void {
                $this->measure('recovery.publish', fn () => $publisher->publish($message), true);
                $first = $this->measure('recovery.claim_initial', fn () => $this->delivery($receiver), true);
                $this->verify($first, $message->id, 0, 1);
                $this->measure('recovery.retry', fn () => $first->retry(0), true);
                $second = $this->measure('recovery.claim_retry', fn () => $this->delivery($receiver), true);
                $this->verify($second, $message->id, 1, 2);
                $this->measure('recovery.fail', fn () => $second->fail(new FailureDetails(FailureReason::Handler)), true);
                $page = $this->measure('recovery.failure_page', fn () => $failures->page(new FailureQuery(1, status: FailureStatus::Pending)), true);
                if (count($page->items) !== 1 || $page->items[0]->messageId !== $message->id) {
                    throw new RuntimeException('Recovery did not retain the expected pending failure.');
                }
                $id = $page->items[0]->failureId;
                $this->measure('recovery.redrive', fn () => $failures->redrive($id), true);
                $third = $this->measure('recovery.claim_redrive', fn () => $this->delivery($receiver), true);
                $this->verify($third, $message->id, 0, 1);
                $this->measure('recovery.acknowledge', $third->acknowledge(...), true);
                $history = $this->measure('recovery.failure_find', fn () => $failures->find($id), true);
                if ($history?->summary->status !== FailureStatus::Redriven) {
                    throw new RuntimeException('Recovery history did not remain redriven.');
                }
            });
        }

        return ['messages' => $this->options['recovery-messages'], 'elapsed_ms' => (hrtime(true) - $start) / 1_000_000];
    }

    /** @return array<string, mixed> */
    private function backlog(Database $database, DriverInterface $driver): array
    {
        $queue = $this->queue($database, 'backlog');
        $publisher = $queue->publisher();
        $receiver = $this->receiver($queue);
        $start = hrtime(true);
        for ($i = 0; $i < $this->options['delayed-backlog']; ++$i) {
            $message = $this->message('delayed-' . $i);
            $this->measure('backlog.delayed_publish', fn () => $publisher->publish($message, 3_600_000), true);
        }
        $seedMilliseconds = (hrtime(true) - $start) / 1_000_000;

        // One additional ready record belongs to the untimed query-plan probe.
        $publisher->publish($this->message('plan-probe'));
        $this->operationCounts['plan_probe.publish'] = 1;
        for ($i = 0; $i < $this->options['backlog-ready']; ++$i) {
            $message = $this->message('backlog-ready-' . $i);
            $this->measure('backlog.ready_publish', fn () => $publisher->publish($message), true);
        }
        $driver->execute('ANALYZE "' . $this->schema . '"."queue_messages"');
        $plans = $this->claimPlans();

        $drainStart = hrtime(true);
        for ($i = 0; $i < $this->options['backlog-ready']; ++$i) {
            $this->measure('backlog.drain_workflow', function () use ($receiver): void {
                $delivery = $this->measure('backlog.claim', fn () => $this->delivery($receiver), true);
                $message = $this->codec->decode($delivery->raw());
                if (!str_starts_with($message->id, 'backlog-ready-') || $message->body !== $this->body) {
                    throw new RuntimeException('A delayed or unexpected record entered the ready workload.');
                }
                $this->measure('backlog.acknowledge', $delivery->acknowledge(...), true);
            });
        }
        $drainMilliseconds = (hrtime(true) - $drainStart) / 1_000_000;
        for ($i = 0; $i < 20; ++$i) {
            if ($this->measure('backlog.idle_poll', $receiver->receive(...), true) !== null) {
                throw new RuntimeException('A future record became eligible during the bounded workload.');
            }
        }
        $remaining = $this->row($driver, 'SELECT count(*) AS remaining FROM "' . $this->schema . '"."queue_messages" WHERE queue_name = :queue AND state = \'ready\' AND available_at > clock_timestamp()', ['queue' => 'backlog']);
        if ((int) $remaining['remaining'] !== $this->options['delayed-backlog']) {
            throw new RuntimeException('The delayed backlog was not preserved.');
        }

        return [
            'delayed_messages' => $this->options['delayed-backlog'],
            'ready_messages' => $this->options['backlog-ready'],
            'seed_elapsed_ms' => $seedMilliseconds,
            'ready_drain_elapsed_ms' => $drainMilliseconds,
            'remaining_future_messages' => (int) $remaining['remaining'],
            'idle_polls' => 20,
            'analyze_before_plan_and_drain' => true,
            'claim_explain_analyze_buffers' => $plans,
        ];
    }

    /** @return list<array<string, mixed>> */
    private function claimPlans(): array
    {
        $trace = new ClaimTrace();
        $driver = $this->driver($this->schema, $trace);
        $queue = $this->queue(new Database('queue-benchmark-plan', '', $driver), 'backlog');
        $receiver = $this->receiver($queue);
        $delivery = $this->delivery($receiver);
        $this->verify($delivery, 'plan-probe', 0, 1);
        $delivery->acknowledge();
        $receiver->close();
        $this->operationCounts['plan_probe.receive'] = 1;
        $this->operationCounts['plan_probe.acknowledge'] = 1;
        if ($trace->queries === []) {
            throw new RuntimeException('The actual candidate SELECTs could not be captured.');
        }
        $queries = array_values(array_unique($trace->queries));
        $plans = [];
        foreach ($queries as $sql) {
            if (!$driver->beginTransaction(DriverInterface::ISOLATION_READ_COMMITTED)) {
                throw new RuntimeException('Could not begin an isolated query-plan probe.');
            }
            try {
                $row = $this->row($driver, 'EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' . $sql);
                $plans[] = [
                    'sql' => $sql,
                    'explain' => json_decode((string) $row['QUERY PLAN'], true, 64, JSON_THROW_ON_ERROR),
                ];
            } finally {
                if (!$driver->rollbackTransaction()) {
                    throw new RuntimeException('The read-only query-plan probe did not confirm rollback.');
                }
            }
        }

        return $plans;
    }

    private function queue(Database $database, string $name): PostgresQueue
    {
        return new PostgresQueue($database, new QueueConfig(
            queue: $name,
            leaseMilliseconds: 30_000,
            maxDeliveries: 5,
            dedupRetentionMilliseconds: 3_600_000,
            failureRetentionMilliseconds: 3_600_000,
            ioTimeoutMilliseconds: 5_000,
            maintenanceLimit: 10,
            pollIntervalMilliseconds: 10,
        ), $this->codec);
    }

    private function receiver(PostgresQueue $queue): ReceiverInterface
    {
        $receiver = $queue->receiver();
        $this->receivers[] = $receiver;
        return $receiver;
    }

    private function driver(string $schema, ?ClaimTrace $trace = null): DriverInterface
    {
        $driver = PostgresDriver::create(new PostgresDriverConfig(
            connection: new DsnConnectionConfig($this->dsn, $this->user, $this->password),
            schema: $schema,
            reconnect: false,
            options: ['logInterpolatedQueries' => $trace !== null],
        ));
        if ($trace !== null) {
            $driver->setLogger($trace);
        }
        $this->drivers[] = $driver;
        return $driver;
    }

    private function message(string $id): Message
    {
        return new Message($id, new MessageType('benchmark.work', 1), $this->body, new Headers(['content-type' => 'application/octet-stream']));
    }

    private function delivery(ReceiverInterface $receiver): DeliveryInterface
    {
        return $receiver->receive() ?? throw new RuntimeException('Expected ready work was not acquired.');
    }

    private function verify(DeliveryInterface $delivery, string $id, int $retryCount, int $deliveryCount): void
    {
        $message = $this->codec->decode($delivery->raw());
        $metadata = $delivery->metadata();
        if ($message->id !== $id || $message->body !== $this->body
            || $metadata->retryCount !== $retryCount || $metadata->deliveryCount !== $deliveryCount) {
            throw new RuntimeException('Public delivery contents/counters did not match the workload.');
        }
    }

    /** @template T @param Closure():T $operation @return T */
    private function measure(string $name, Closure $operation, bool $apiCall = false): mixed
    {
        $start = hrtime(true);
        $result = $operation();
        $this->samples[$name][] = (hrtime(true) - $start) / 1_000_000;
        if ($apiCall) {
            $this->operationCounts[$name] = ($this->operationCounts[$name] ?? 0) + 1;
        }
        return $result;
    }

    /** @param non-empty-list<float> $samples @return array<string, int|float> */
    private function statistics(array $samples): array
    {
        sort($samples, SORT_NUMERIC);
        $count = count($samples);
        $sum = array_sum($samples);
        return [
            'count' => $count,
            'sum_ms' => $sum,
            'minimum_ms' => $samples[0],
            'mean_ms' => $sum / $count,
            'p50_ms' => $samples[(int) ceil(0.50 * $count) - 1],
            'p95_ms' => $samples[(int) ceil(0.95 * $count) - 1],
            'p99_ms' => $samples[(int) ceil(0.99 * $count) - 1],
            'maximum_ms' => $samples[$count - 1],
        ];
    }

    /** @param array<string, mixed> $parameters @return array<string, mixed> */
    private function row(DriverInterface $driver, string $sql, array $parameters = []): array
    {
        $statement = $driver->query($sql, $parameters);
        try {
            $row = $statement->fetch();
            if ($row === false) {
                throw new RuntimeException('A required benchmark metadata row was absent.');
            }
            return $row;
        } finally {
            $statement->close();
        }
    }

    private function sourceFingerprint(): string
    {
        $corePath = InstalledVersions::getInstallPath('componenta/queue');
        if ($corePath === null) {
            throw new RuntimeException('The core package install path is unavailable.');
        }
        $files = [];
        foreach (['core/src' => $corePath . '/src', 'adapter/src' => $this->root . '/src', 'adapter/resources' => $this->root . '/resources'] as $prefix => $directory) {
            $iterator = new \RecursiveIteratorIterator(new \RecursiveDirectoryIterator($directory, \FilesystemIterator::SKIP_DOTS));
            foreach ($iterator as $file) {
                if ($file instanceof \SplFileInfo && $file->isFile() && in_array($file->getExtension(), ['php', 'sql'], true)) {
                    $files[$prefix . substr($file->getPathname(), strlen($directory))] = $file->getPathname();
                }
            }
        }
        foreach (['composer.json', 'composer.lock', 'composer.dev.lock'] as $name) {
            if (is_file($this->root . '/' . $name)) {
                $files['adapter/' . $name] = $this->root . '/' . $name;
            }
        }
        $files['core/composer.json'] = $corePath . '/composer.json';
        ksort($files, SORT_STRING);
        $hash = hash_init('sha256');
        foreach ($files as $name => $path) {
            hash_update($hash, $name . "\0" . hash_file('sha256', $path) . "\n");
        }
        return hash_final($hash);
    }

    /** @param array<string, int> $usage */
    private static function cpuSeconds(array $usage, string $kind): float
    {
        return $usage['ru_' . $kind . '.tv_sec'] + $usage['ru_' . $kind . '.tv_usec'] / 1_000_000;
    }
}

/** @return array<string, int> */
function options(): array
{
    $arguments = getopt('', ['messages:', 'recovery-messages:', 'delayed-backlog:', 'backlog-ready:', 'warmup:', 'payload-bytes:', 'help']);
    if (isset($arguments['help'])) {
        fwrite(STDOUT, "Required environment: QUEUE_BENCH_DSN (pgsql DSN with finite connect_timeout).\n"
            . "Optional credentials: QUEUE_BENCH_USER, QUEUE_BENCH_PASSWORD.\n"
            . "Options/defaults: --messages=200 --recovery-messages=50 --delayed-backlog=10000\n"
            . "                  --backlog-ready=200 --warmup=20 --payload-bytes=256\n"
            . "A fresh schema is created and dropped. JSON is emitted after successful cleanup.\n");
        exit(0);
    }
    $defaults = ['messages' => 200, 'recovery-messages' => 50, 'delayed-backlog' => 10_000, 'backlog-ready' => 200, 'warmup' => 20, 'payload-bytes' => 256];
    foreach ($defaults as $name => $default) {
        $value = $arguments[$name] ?? $default;
        if (!is_int($value) && (!is_string($value) || preg_match('/\A[0-9]+\z/', $value) !== 1)) {
            throw new InvalidArgumentException('Expected a positive integer for --' . $name . '.');
        }
        $value = filter_var($value, FILTER_VALIDATE_INT);
        $maximum = $name === 'payload-bytes' ? 1_048_576 : 1_000_000;
        if ($value === false || $value < 1 || $value > $maximum) {
            throw new InvalidArgumentException('The value for --' . $name . ' is outside the supported workload bounds.');
        }
        $defaults[$name] = $value;
    }
    return $defaults;
}

try {
    $options = options();
    $dsn = getenv('QUEUE_BENCH_DSN');
    if ($dsn === false || !str_starts_with($dsn, 'pgsql:') || preg_match('/(?:^|;)connect_timeout=[1-9][0-9]*(?:;|$)/', substr($dsn, 6)) !== 1) {
        throw new InvalidArgumentException('Set QUEUE_BENCH_DSN to an explicit PostgreSQL DSN with a positive connect_timeout; no test/production DSN is inherited.');
    }
    $workload = new Workload(
        $dsn,
        getenv('QUEUE_BENCH_USER') === false ? null : (string) getenv('QUEUE_BENCH_USER'),
        getenv('QUEUE_BENCH_PASSWORD') === false ? null : (string) getenv('QUEUE_BENCH_PASSWORD'),
        $options,
        $root,
    );
    fwrite(STDOUT, json_encode($workload->run(), JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR) . "\n");
} catch (Throwable $error) {
    // Connection errors may contain user/host details. Keep the default failure output bounded.
    fwrite(STDERR, 'Benchmark aborted: ' . $error::class . '. ' . ($error instanceof InvalidArgumentException ? $error->getMessage() : 'No successful measurement is reported.') . "\n");
    exit(1);
}

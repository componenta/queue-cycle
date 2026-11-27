<?php

declare(strict_types=1);

/**
 * Idle cleanup scaling with publicly published future messages.
 * Requires an explicit QUEUE_BENCH_DSN; only a new random schema is modified.
 * Run: php benchmarks/CleanupWorkload.php --sizes=100,1000,10000 --samples=20
 */

namespace Componenta\Queue\Cycle\Benchmarks\Cleanup;

use Componenta\Queue\Codec\JsonMessageCodec;
use Componenta\Queue\Cycle\PostgresQueue;
use Componenta\Queue\Cycle\QueueConfig;
use Componenta\Queue\Cycle\Schema;
use Componenta\Queue\Message;
use Componenta\Queue\MessageLimits;
use Componenta\Queue\MessageType;
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

require dirname(__DIR__) . '/vendor/autoload.php';

final class SelectionTrace extends AbstractLogger
{
    public bool $enabled = false;
    public ?string $sql = null;

    public function log($level, string|Stringable $message, array $context = []): void
    {
        $sql = (string) $message;
        if ($this->enabled && str_contains($sql, 'SELECT m.row_id, m.state') && str_contains($sql, 'FOR UPDATE OF m SKIP LOCKED')) {
            $this->sql = $sql;
        }
    }
}

/** @param array<string,mixed> $parameters @return array<string,mixed> */
function row(DriverInterface $driver, string $sql, array $parameters = []): array
{
    $statement = $driver->query($sql, $parameters);
    try {
        $result = $statement->fetch();
        if ($result === false) {
            throw new RuntimeException('A required benchmark metadata row was absent.');
        }
        return $result;
    } finally {
        $statement->close();
    }
}

function sourceFingerprint(): string
{
    $core = InstalledVersions::getInstallPath('componenta/queue') ?? throw new RuntimeException('The installed core path is absent.');
    $files = [];
    foreach (['core' => $core . '/src', 'adapter' => dirname(__DIR__) . '/src', 'schema' => dirname(__DIR__) . '/resources'] as $name => $path) {
        $iterator = new \RecursiveIteratorIterator(new \RecursiveDirectoryIterator($path, \FilesystemIterator::SKIP_DOTS));
        foreach ($iterator as $file) {
            if ($file instanceof \SplFileInfo && $file->isFile()) {
                $files[$name . substr($file->getPathname(), strlen($path))] = hash_file('sha256', $file->getPathname());
            }
        }
    }
    ksort($files, SORT_STRING);
    return hash('sha256', json_encode($files, JSON_THROW_ON_ERROR));
}

try {
    $arguments = getopt('', ['sizes:', 'samples:', 'help']);
    if (isset($arguments['help'])) {
        fwrite(STDOUT, "Required: QUEUE_BENCH_DSN (pgsql with finite connect_timeout).\n"
            . "Optional: QUEUE_BENCH_USER, QUEUE_BENCH_PASSWORD.\n"
            . "Options: --sizes=100,1000,10000 --samples=20. Warmup: 3 calls per size.\n"
            . "Measures cleanup(1) with only future work; creates and drops a random schema.\n");
        exit(0);
    }
    $sizesInput = $arguments['sizes'] ?? '100,1000,10000';
    $samplesInput = $arguments['samples'] ?? '20';
    if (!is_string($sizesInput) || preg_match('/\A[1-9][0-9]*(?:,[1-9][0-9]*)*\z/', $sizesInput) !== 1
        || !is_string($samplesInput) || preg_match('/\A[1-9][0-9]*\z/', $samplesInput) !== 1) {
        throw new InvalidArgumentException('Expected positive decimal sizes and sample count.');
    }
    $sizes = array_map(static function (string $value): int {
        $size = filter_var($value, FILTER_VALIDATE_INT);
        if ($size === false || $size > 1_000_000) {
            throw new InvalidArgumentException('Each size must be within 1..1000000.');
        }
        return $size;
    }, explode(',', $sizesInput));
    $sizes = array_values(array_unique($sizes));
    sort($sizes, SORT_NUMERIC);
    $samples = filter_var($samplesInput, FILTER_VALIDATE_INT);
    if ($samples === false || $samples < 5 || $samples > 1_000) {
        throw new InvalidArgumentException('The sample count must be within 5..1000.');
    }
    $dsn = getenv('QUEUE_BENCH_DSN');
    if ($dsn === false || !str_starts_with($dsn, 'pgsql:') || preg_match('/(?:^|;)connect_timeout=[1-9][0-9]*(?:;|$)/', substr($dsn, 6)) !== 1) {
        throw new InvalidArgumentException('Set an explicit PostgreSQL QUEUE_BENCH_DSN with finite connect_timeout.');
    }
    $connection = new DsnConnectionConfig($dsn, getenv('QUEUE_BENCH_USER') === false ? null : (string) getenv('QUEUE_BENCH_USER'), getenv('QUEUE_BENCH_PASSWORD') === false ? null : (string) getenv('QUEUE_BENCH_PASSWORD'));
    $admin = PostgresDriver::create(new PostgresDriverConfig(connection: $connection, schema: 'public', reconnect: false));
    $schema = 'queue_cleanup_bench_' . bin2hex(random_bytes(12));
    $quoted = '"' . $schema . '"';
    $driver = null;
    $created = false;
    $sourceBefore = sourceFingerprint();
    $results = [];
    $failure = null;
    try {
        $environment = row($admin, "SELECT version() AS version, current_setting('fsync') AS fsync,
            current_setting('synchronous_commit') AS synchronous_commit, current_setting('full_page_writes') AS full_page_writes");
        $admin->execute('CREATE SCHEMA ' . $quoted);
        $created = true;
        $driver = PostgresDriver::create(new PostgresDriverConfig(connection: $connection, schema: $schema, reconnect: false, options: ['logInterpolatedQueries' => true]));
        $trace = new SelectionTrace();
        $driver->setLogger($trace);
        $database = new Database('queue-cleanup-benchmark', '', $driver);
        Schema::install($database);
        $codec = new JsonMessageCodec(new MessageLimits(1_000_000, 2_000_000, 4096, 8));
        $queue = new PostgresQueue($database, new QueueConfig('jobs', 5_000, 3, 60_000, 60_000, 5_000, 10, 10), $codec);
        $publisher = $queue->publisher();
        $seeded = 0;
        foreach ($sizes as $size) {
            while ($seeded < $size) {
                $publisher->publish(new Message('future-' . ++$seeded, new MessageType('benchmark.work', 1), "\0\xffbody"), 3_600_000);
            }
            $driver->execute('ANALYZE ' . $quoted . '."queue_messages"');
            $driver->execute('ANALYZE ' . $quoted . '."queue_failures"');
            $times = [];
            for ($attempt = 0; $attempt < $samples + 3; ++$attempt) {
                $start = hrtime(true);
                $cleanup = $queue->cleanup(1);
                $elapsed = (hrtime(true) - $start) / 1_000_000;
                if ($cleanup->messagesDeleted !== 0 || $cleanup->failuresDeleted !== 0 || $cleanup->historiesDetached !== 0) {
                    throw new RuntimeException('Idle cleanup changed future work.');
                }
                if ($attempt >= 3) {
                    $times[] = $elapsed;
                }
            }
            $trace->enabled = true;
            $queue->cleanup(1);
            $trace->enabled = false;
            $sql = $trace->sql ?? throw new RuntimeException('The actual cleanup selection was not captured.');
            if (!$driver->beginTransaction(DriverInterface::ISOLATION_READ_COMMITTED)) {
                throw new RuntimeException('The query-plan transaction did not start.');
            }
            try {
                $plan = json_decode((string) row($driver, 'EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' . $sql)['QUERY PLAN'], true, 64, JSON_THROW_ON_ERROR);
            } finally {
                if (!$driver->rollbackTransaction()) {
                    throw new RuntimeException('The query-plan transaction did not roll back.');
                }
            }
            sort($times, SORT_NUMERIC);
            $remaining = row($driver, 'SELECT count(*) AS count FROM ' . $quoted . '."queue_messages" WHERE state = \'ready\' AND available_at > clock_timestamp()')['count'];
            if ((int) $remaining !== $size) {
                throw new RuntimeException('The future backlog was not preserved.');
            }
            $results[] = ['future_rows' => $size, 'cleanup_limit' => 1, 'samples' => $samples, 'effects' => 0,
                'mean_ms' => array_sum($times) / $samples, 'p50_ms' => $times[(int) ceil($samples * 0.5) - 1], 'p95_ms' => $times[(int) ceil($samples * 0.95) - 1],
                'sql' => $sql, 'explain_analyze_buffers' => $plan];
        }
    } catch (Throwable $error) {
        $failure = $error;
    } finally {
        if ($driver !== null) {
            try {
                if ($driver->getTransactionLevel() > 0 && !$driver->rollbackTransaction()) {
                    throw new RuntimeException('Benchmark cleanup could not confirm rollback.');
                }
            } catch (Throwable $error) {
                $failure ??= $error;
            }
            try {
                $driver->disconnect();
            } catch (Throwable $error) {
                $failure ??= $error;
            }
        }
        try {
            if ($created) {
                $admin->execute('DROP SCHEMA ' . $quoted . ' CASCADE');
            }
        } catch (Throwable $error) {
            $failure ??= $error;
        }
        try {
            $admin->disconnect();
        } catch (Throwable $error) {
            $failure ??= $error;
        }
    }
    if ($failure !== null) {
        throw $failure;
    }
    fwrite(STDOUT, json_encode(['method' => 'single caller; sequential synchronous cleanup; monotonic hrtime; nearest-rank percentiles',
        'measured_at_utc' => gmdate('Y-m-d\TH:i:s\Z'), 'php_version' => PHP_VERSION,
        'cycle_database_version' => InstalledVersions::getPrettyVersion('cycle/database'), 'database' => $environment,
        'schema_cleanup_confirmed' => true, 'source_sha256_before' => $sourceBefore, 'source_sha256_after' => sourceFingerprint(),
        'benchmark_sha256' => hash_file('sha256', __FILE__), 'results' => $results], JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR) . "\n");
} catch (Throwable $error) {
    fwrite(STDERR, 'Cleanup benchmark aborted: ' . $error::class . '. No successful measurement is reported. See --help for required options.' . "\n");
    exit(1);
}

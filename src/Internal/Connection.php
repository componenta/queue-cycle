<?php

declare(strict_types=1);

namespace Componenta\Queue\Cycle\Internal;

use Componenta\Queue\Exception\DeliveryLostException;
use Componenta\Queue\Exception\FailureNotFoundException;
use Componenta\Queue\Exception\FailureStateConflictException;
use Componenta\Queue\Exception\IndeterminateOperationException;
use Componenta\Queue\Exception\MessageConflictException;
use Componenta\Queue\Exception\TransportException;
use Componenta\Queue\Exception\UnsupportedFeatureException;
use Componenta\Queue\OperationOutcome;
use Cycle\Database\DatabaseInterface;
use Cycle\Database\Driver\DriverInterface;

/** @internal Owns transaction phases without borrowing the read driver or exposing native PDO. */
final class Connection
{
    public const string INSTALLATION_MARKER_PREFIX = 'componenta.queue.failure-store.v1:';

    private readonly DriverInterface $driver;
    private ?string $schema = null;

    public function __construct(DatabaseInterface $database, private readonly int $timeoutMilliseconds)
    {
        $this->driver = $database->getDriver(DatabaseInterface::WRITE);
        if ($this->driver->getType() !== 'Postgres' || $this->driver->isReadonly()) {
            throw new UnsupportedFeatureException('This adapter requires a PostgreSQL write driver.');
        }
    }

    /**
     * @template T
     * @param callable():T $operation
     * @return T
     */
    public function transaction(callable $operation): mixed
    {
        if ($this->driver->getTransactionLevel() !== 0) {
            throw new TransportException('Ordinary queue operations cannot join an application transaction.');
        }
        $begun = false;
        $commitAttempted = false;
        try {
            $begun = $this->driver->beginTransaction(DriverInterface::ISOLATION_READ_COMMITTED);
            if (!$begun) {
                throw new \RuntimeException('The database declined to begin a transaction.');
            }
            $this->row("SELECT set_config('statement_timeout', :timeout, true), set_config('lock_timeout', :lock_timeout, true)", [
                'timeout' => $this->timeoutMilliseconds . 'ms',
                'lock_timeout' => $this->timeoutMilliseconds . 'ms',
            ]);
            $this->validatePlatform();
            $result = $operation();
            $commitAttempted = true;
            if (!$this->driver->commitTransaction()) {
                throw new \RuntimeException('The database did not confirm transaction commit.');
            }
            return $result;
        } catch (\Throwable $error) {
            if ($commitAttempted) {
                throw new IndeterminateOperationException('The queue transaction commit outcome is unknown.', $error);
            }
            if ($begun) {
                try {
                    if (!$this->driver->rollbackTransaction()) {
                        throw new \RuntimeException('The database did not confirm rollback.');
                    }
                } catch (\Throwable $rollbackError) {
                    throw new IndeterminateOperationException('The queue transaction could not be confirmed rolled back.', new RollbackFailure($error, $rollbackError));
                }
            }
            if ($error instanceof TransportException || $error instanceof DeliveryLostException || $error instanceof MessageConflictException
                || $error instanceof FailureStateConflictException || $error instanceof FailureNotFoundException || $error instanceof UnsupportedFeatureException
                || $error instanceof \InvalidArgumentException) {
                throw $error;
            }
            throw new TransportException('The queue transaction was not applied.', OperationOutcome::NotApplied, $error);
        }
    }

    /**
     * @template T
     * @param callable():T $operation
     * @return T
     */
    public function stage(callable $operation): mixed
    {
        if ($this->driver->getTransactionLevel() < 1) {
            throw new TransportException('Transactional staging requires an active application transaction.');
        }
        // The application owns transaction settings, commit, rollback and recovery here.
        try {
            $this->validatePlatform();
            return $operation();
        } catch (TransportException|MessageConflictException|UnsupportedFeatureException|\InvalidArgumentException $error) {
            throw $error;
        } catch (\Throwable $error) {
            throw new IndeterminateOperationException('Staging failed; the application must resolve or roll back its transaction.', $error);
        }
    }

    /** @param array<string,mixed> $parameters */
    public function row(string $sql, array $parameters = []): ?Row
    {
        if ($sql === '') {
            throw new \InvalidArgumentException('A database query cannot be empty.');
        }
        $statement = $this->driver->query($sql, $parameters);
        try {
            $row = $statement->fetch();
            return $row === false ? null : new Row($row);
        } finally {
            $statement->close();
        }
    }

    /**
     * @param array<string,mixed> $parameters
     * @return list<Row>
     */
    public function rows(string $sql, array $parameters = []): array
    {
        if ($sql === '') {
            throw new \InvalidArgumentException('A database query cannot be empty.');
        }
        $statement = $this->driver->query($sql, $parameters);
        try {
            $rows = [];
            foreach ($statement->fetchAll() as $row) {
                $rows[] = new Row($row);
            }
            return $rows;
        } finally {
            $statement->close();
        }
    }

    /** @param array<string,mixed> $parameters */
    public function execute(string $sql, array $parameters = []): int
    {
        if ($sql === '') {
            throw new \InvalidArgumentException('A database statement cannot be empty.');
        }
        return $this->driver->execute($sql, $parameters);
    }

    public function table(string $name): string
    {
        if ($this->schema === null) {
            throw new \LogicException('A transaction must establish the configured database schema first.');
        }
        return '"' . str_replace('"', '""', $this->schema) . '"."' . $name . '"';
    }

    public function scope(string $queue): string
    {
        $row = $this->row("SELECT current_database() AS database, c.oid::text AS table_oid,
            obj_description(c.oid, 'pg_class') AS marker FROM pg_class AS c
            WHERE c.oid = CAST(:table AS regclass)", ['table' => $this->table('queue_failures')]);
        $marker = $row?->nullableString('marker');
        if ($row === null || $marker === null || preg_match('/\Acomponenta\.queue\.failure-store\.v1:[a-f0-9]{32}\z/', $marker) !== 1) {
            throw new UnsupportedFeatureException('The failure table is missing its queue installation identity.');
        }
        return hash('sha256', $row->string('database') . "\0" . $this->schema . "\0" . $row->string('table_oid') . "\0" . $marker . "\0" . $queue);
    }

    public function now(): \DateTimeImmutable
    {
        $row = $this->row("SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') AS instant");
        return new \DateTimeImmutable(($row ?? throw new \UnexpectedValueException('The database clock returned no row.'))->string('instant'), new \DateTimeZone('UTC'));
    }

    public function after(\DateTimeImmutable $instant, int $milliseconds): \DateTimeImmutable
    {
        $row = $this->row("SELECT to_char((CAST(:instant AS timestamptz) + CAST(:duration AS interval)) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') AS instant", [
            'instant' => self::format($instant), 'duration' => $milliseconds . ' milliseconds',
        ]);
        return new \DateTimeImmutable(($row ?? throw new \UnexpectedValueException('The database timestamp calculation returned no row.'))->string('instant'), new \DateTimeZone('UTC'));
    }

    public static function format(\DateTimeImmutable $instant): string
    {
        return $instant->format('Y-m-d H:i:s.uP');
    }

    private function validatePlatform(): void
    {
        $row = $this->row("SELECT current_setting('server_version_num') AS version, current_setting('server_encoding') AS server_encoding, current_setting('client_encoding') AS client_encoding, current_setting('block_size') AS block_size, current_schema() AS schema");
        if ($row === null || $row->integer('version') < 160000 || $row->integer('block_size') < 8192 || $row->string('server_encoding') !== 'UTF8' || $row->string('client_encoding') !== 'UTF8' || $row->nullableString('schema') === null) {
            throw new UnsupportedFeatureException('PostgreSQL 16+, UTF8 server/client encodings, an 8 KiB+ block size and a configured schema are required.');
        }
        $this->schema ??= $row->string('schema');
    }
}

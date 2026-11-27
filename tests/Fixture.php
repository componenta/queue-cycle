<?php

declare(strict_types=1);

namespace Componenta\Queue\Cycle\Tests;

use Componenta\Queue\Codec\JsonMessageCodec;
use Componenta\Queue\Cycle\PostgresQueue;
use Componenta\Queue\Cycle\QueueConfig;
use Componenta\Queue\Cycle\Schema;
use Componenta\Queue\Headers;
use Componenta\Queue\Message;
use Componenta\Queue\MessageLimits;
use Componenta\Queue\MessageType;
use Cycle\Database\Config\Postgres\DsnConnectionConfig;
use Cycle\Database\Config\PostgresDriverConfig;
use Cycle\Database\Database;
use Cycle\Database\DatabaseInterface;
use Cycle\Database\Driver\DriverInterface;
use Cycle\Database\Driver\Postgres\PostgresDriver;

final class Fixture
{
    public readonly string $schema;
    public readonly DatabaseInterface $database;
    public readonly JsonMessageCodec $codec;
    private readonly DriverInterface $admin;
    /** @var list<DriverInterface> */
    private array $drivers = [];

    public function __construct()
    {
        $this->schema = 'queue_test_' . bin2hex(random_bytes(8));
        $this->admin = $this->newDriver('public');
        $this->admin->execute('CREATE SCHEMA "' . $this->schema . '"');
        $this->database = $this->newDatabase();
        $this->codec = new JsonMessageCodec(new MessageLimits(1_000_000, 2_000_000, 4096, 8));
        Schema::install($this->database);
    }

    public function newDatabase(?string $dsn = null): DatabaseInterface
    {
        return new Database('queue-tests', '', $this->newDriver($this->schema, $dsn));
    }

    /** @param array<string,int|string|null> $overrides */
    public function queue(array $overrides = [], ?DatabaseInterface $database = null): PostgresQueue
    {
        $config = new QueueConfig(...array_replace([
            'queue' => 'jobs',
            'leaseMilliseconds' => 5_000,
            'maxDeliveries' => 3,
            'dedupRetentionMilliseconds' => 60_000,
            'failureRetentionMilliseconds' => 60_000,
            'ioTimeoutMilliseconds' => 2_000,
            'maintenanceLimit' => 10,
            'pollIntervalMilliseconds' => 10,
        ], $overrides));

        return new PostgresQueue($database ?? $this->database, $config, $this->codec);
    }

    public function message(string $id = 'message-1', string $body = "\0\xffbody"): Message
    {
        return new Message($id, new MessageType('test.work', 1), $body, new Headers(['content-type' => 'application/octet-stream']));
    }

    public function table(string $name = 'queue_messages'): string
    {
        return '"' . $this->schema . '"."' . $name . '"';
    }

    /** @param array<string,mixed> $parameters */
    public function execute(string $sql, array $parameters = []): int
    {
        return $this->database->getDriver(DatabaseInterface::WRITE)->execute($sql, $parameters);
    }

    /** @param array<string,mixed> $parameters @return array<string,mixed> */
    public function row(string $sql, array $parameters = []): array
    {
        $statement = $this->database->getDriver(DatabaseInterface::WRITE)->query($sql, $parameters);
        try {
            $row = $statement->fetch();
            if ($row === false) {
                throw new \RuntimeException('The test expected a database row.');
            }
            return $row;
        } finally {
            $statement->close();
        }
    }

    public function expire(string $id): void
    {
        $this->execute('UPDATE ' . $this->table() . " SET lease_until = clock_timestamp() WHERE logical_id = :id AND state = 'leased'", ['id' => $id]);
    }

    public function close(): void
    {
        foreach ($this->drivers as $driver) {
            if ($driver->getTransactionLevel() > 0) {
                $driver->rollbackTransaction();
            }
            $driver->disconnect();
        }
        $this->admin->execute('DROP SCHEMA "' . $this->schema . '" CASCADE');
        $this->admin->disconnect();
    }

    private function newDriver(string $schema, ?string $dsn = null): DriverInterface
    {
        $driver = PostgresDriver::create(new PostgresDriverConfig(
            connection: new DsnConnectionConfig(
                dsn: $dsn ?? (string) getenv('QUEUE_TEST_DSN'),
                user: (string) (getenv('QUEUE_TEST_USER') ?: 'queue_test'),
                password: getenv('QUEUE_TEST_PASSWORD') === false ? null : (string) getenv('QUEUE_TEST_PASSWORD'),
            ),
            schema: $schema,
            reconnect: false,
        ));
        if ($schema !== 'public') {
            $this->drivers[] = $driver;
        }
        return $driver;
    }
}

<?php

declare(strict_types=1);

use Componenta\Queue\Codec\JsonMessageCodec;
use Componenta\Queue\Cycle\PostgresQueue;
use Componenta\Queue\Cycle\QueueConfig;
use Componenta\Queue\FailureDetails;
use Componenta\Queue\FailureReason;
use Componenta\Queue\Headers;
use Componenta\Queue\Message;
use Componenta\Queue\MessageLimits;
use Componenta\Queue\MessageType;
use Cycle\Database\Config\Postgres\DsnConnectionConfig;
use Cycle\Database\Config\PostgresDriverConfig;
use Cycle\Database\Database;
use Cycle\Database\Driver\Postgres\PostgresDriver;

require dirname(__DIR__) . '/bootstrap.php';
$settings = json_decode(base64_decode($argv[1], true), true, 16, JSON_THROW_ON_ERROR);
$driver = PostgresDriver::create(new PostgresDriverConfig(
    connection: new DsnConnectionConfig(dsn: (string) getenv('QUEUE_TEST_DSN'), user: (string) (getenv('QUEUE_TEST_USER') ?: 'queue_test'), password: getenv('QUEUE_TEST_PASSWORD') === false ? null : (string) getenv('QUEUE_TEST_PASSWORD')),
    schema: $settings['schema'],
    reconnect: false,
));
$database = new Database('queue-tests', '', $driver);
$codec = new JsonMessageCodec(new MessageLimits(1_000_000, 2_000_000, 4096, 8));
$config = new QueueConfig(...array_replace([
    'queue' => 'jobs', 'leaseMilliseconds' => 5_000, 'maxDeliveries' => 3,
    'dedupRetentionMilliseconds' => 60_000, 'failureRetentionMilliseconds' => 60_000,
    'ioTimeoutMilliseconds' => 2_000, 'maintenanceLimit' => 10, 'pollIntervalMilliseconds' => 10,
], $settings['overrides']));
$queue = new PostgresQueue($database, $config, $codec);
$receiver = $queue->receiver();
$delivery = null;
$table = '"' . $settings['schema'] . '"."queue_messages"';
$row = static function (string $sql, array $parameters = []) use ($driver): array {
    $statement = $driver->query($sql, $parameters);
    try {
        return $statement->fetch();
    } finally {
        $statement->close();
    }
};
$write = static function (array $message): void {
    fwrite(STDOUT, json_encode($message, JSON_THROW_ON_ERROR) . "\n");
    fflush(STDOUT);
};
$write(['pid' => (int) $row('SELECT pg_backend_pid() AS pid')['pid']]);
while (($line = fgets(STDIN)) !== false) {
    $command = json_decode($line, true, 16, JSON_THROW_ON_ERROR);
    try {
        switch ($command['op']) {
            case 'publish':
                $queue->publisher()->publish(new Message($command['id'], new MessageType('test.work', 1), base64_decode($command['body'] ?? base64_encode("\0\xffbody"), true), new Headers(['content-type' => 'application/octet-stream'])), $command['delay'] ?? 0);
                $write(['ok' => true]);
                break;
            case 'receive':
                $delivery = $receiver->receive($command['wait'] ?? 0);
                $write(['id' => $delivery === null ? null : $codec->decode($delivery->raw())->id, 'count' => $delivery?->metadata()->deliveryCount]);
                break;
            case 'ack': $delivery->acknowledge();
                $write(['ok' => true]);
                break;
            case 'retry': $delivery->retry($command['delay'] ?? 0);
                $write(['ok' => true]);
                break;
            case 'fail': $delivery->fail(new FailureDetails(FailureReason::Handler));
                $write(['ok' => true]);
                break;
            case 'redrive': $queue->failures()->redrive($command['id'], $command['delay'] ?? 0);
                $write(['ok' => true]);
                break;
            case 'discard': $queue->failures()->discard($command['id']);
                $write(['ok' => true]);
                break;
            case 'cleanup':
                $result = $queue->cleanup($command['limit'] ?? 10);
                $write(['messages' => $result->messagesDeleted, 'failures' => $result->failuresDeleted, 'detached' => $result->historiesDetached]);
                break;
            case 'hold_until_expired':
                $driver->beginTransaction();
                $row("SELECT row_id FROM $table WHERE logical_id = :id FOR UPDATE", ['id' => $command['id']]);
                $write(['locked' => true]);
                $deadline = hrtime(true) + 5_000_000_000;
                while (!$row("SELECT lease_until <= clock_timestamp() AS expired FROM $table WHERE logical_id = :id", ['id' => $command['id']])['expired']) {
                    if (hrtime(true) > $deadline) {
                        throw new RuntimeException('Lease did not expire within test deadline.');
                    }
                    usleep(1_000);
                }
                $driver->commitTransaction();
                $write(['released' => true]);
                break;
            case 'exit': $write(['exited' => true]);
                exit(0);
            default: throw new RuntimeException('Unknown worker test command.');
        }
    } catch (Throwable $error) {
        $write(['error' => $error::class, 'message' => $error->getMessage()]);
    }
}

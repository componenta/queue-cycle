<?php

declare(strict_types=1);

use Componenta\Queue\Cycle\QueueConfig;
use Componenta\Queue\Cycle\Schema;
use Componenta\Queue\Cycle\Tests\Fixture;
use Componenta\Queue\Exception\InvalidMessageException;
use Componenta\Queue\Exception\MessageConflictException;
use Componenta\Queue\Exception\UnsupportedFeatureException;
use Componenta\Queue\Headers;
use Componenta\Queue\FailureDetails;
use Componenta\Queue\FailureQuery;
use Componenta\Queue\FailureReason;
use Componenta\Queue\Message;
use Cycle\Database\Config\Postgres\DsnConnectionConfig;
use Cycle\Database\Config\PostgresDriverConfig;
use Cycle\Database\Database;
use Cycle\Database\DatabaseInterface;
use Cycle\Database\Driver\Postgres\PostgresDriver;

it('uses only the authoritative write driver when the configured read replica is empty', function (): void {
    $replica = new Fixture();
    try {
        $database = new Database('split', '', $this->fixture->database->getDriver(DatabaseInterface::WRITE), $replica->database->getDriver(DatabaseInterface::WRITE));
        $queue = $this->fixture->queue(database: $database);
        $queue->publisher()->publish($this->fixture->message());
        $delivery = $queue->receiver()->receive();
        expect($delivery)->not->toBeNull();
        $delivery->acknowledge();
        expect((int) $replica->row('SELECT count(*) AS count FROM ' . $replica->table())['count'])->toBe(0);
        expect($queue->receiver()->receive())->toBeNull();
    } finally {
        $replica->close();
    }
});

it('does not close the borrowed connection when a receiver closes', function (): void {
    $queue = $this->fixture->queue();
    $pid = (int) $this->fixture->row('SELECT pg_backend_pid() AS pid')['pid'];
    $queue->publisher()->publish($this->fixture->message());
    $receiver = $queue->receiver();
    $receiver->receive();
    $receiver->close();
    expect((int) $this->fixture->row('SELECT pg_backend_pid() AS pid')['pid'])->toBe($pid);
    expect($this->fixture->row('SELECT state FROM ' . $this->fixture->table())['state'])->toBe('leased');
});

it('rejects non-UTF8 connection configuration before consuming and works after correction', function (): void {
    $queue = $this->fixture->queue();
    $queue->publisher()->publish($this->fixture->message());
    $this->fixture->execute("SET client_encoding = 'LATIN1'");
    expect(fn () => $queue->receiver()->receive())->toThrow(UnsupportedFeatureException::class);
    $this->fixture->execute("SET client_encoding = 'UTF8'");
    expect($queue->receiver()->receive()->metadata()->deliveryCount)->toBe(1);
});

it('reads timestamps independently of session DateStyle without changing the borrowed session', function (): void {
    $queue = $this->fixture->queue();
    $this->fixture->execute("SET DateStyle TO 'SQL, DMY'");
    $queue->publisher()->publish($this->fixture->message());
    // A fixed past availability makes an ambiguous/non-ISO date visible on every test day.
    $this->fixture->execute('UPDATE ' . $this->fixture->table() . " SET available_at = '2000-01-13 12:34:56.123456+00'::timestamptz");
    $receiver = $queue->receiver();

    $delivery = $receiver->receive();
    expect($delivery)->not->toBeNull();
    $stored = $this->fixture->row('SELECT to_char(received_at AT TIME ZONE \'UTC\', \'YYYY-MM-DD HH24:MI:SS.US\') AS received FROM ' . $this->fixture->table());
    expect($delivery->metadata()->receivedAt->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d H:i:s.u'))->toBe($stored['received']);
    expect($delivery->lease()->renew(5_000)->expiresAt)->toBeGreaterThan($delivery->metadata()->receivedAt);
    $delivery->acknowledge();
    expect($this->fixture->row("SELECT current_setting('DateStyle') AS style")['style'])->toBe('SQL, DMY');
    expect(fn () => $queue->publisher()->publish($this->fixture->message(body: 'conflicting original')))
        ->toThrow(MessageConflictException::class);
    expect($this->fixture->row("SELECT current_setting('DateStyle') AS style")['style'])->toBe('SQL, DMY');

    $writer = $this->fixture->database->getDriver(DatabaseInterface::WRITE);
    $writer->beginTransaction();
    $queue->transactionalPublisher()->stage($this->fixture->message('staged'));
    expect($this->fixture->row("SELECT current_setting('DateStyle') AS style")['style'])->toBe('SQL, DMY');
    $writer->rollbackTransaction();
    expect($receiver->receive())->toBeNull();
});

it('rejects a readonly driver before acquiring anything', function (): void {
    $readonly = PostgresDriver::create(new PostgresDriverConfig(
        connection: new DsnConnectionConfig(dsn: (string) getenv('QUEUE_TEST_DSN'), user: (string) (getenv('QUEUE_TEST_USER') ?: 'queue_test')),
        schema: $this->fixture->schema,
        readonly: true,
        reconnect: false,
    ));
    expect(fn () => $this->fixture->queue(database: new Database('readonly', '', $readonly)))->toThrow(UnsupportedFeatureException::class);
    $queue = $this->fixture->queue();
    $queue->publisher()->publish($this->fixture->message());
    expect($queue->receiver()->receive())->not->toBeNull();
});

it('enforces queue and logical identity byte limits without normalizing Unicode or case', function (): void {
    expect(fn () => $this->fixture->queue(['queue' => str_repeat('é', 65)]))->toThrow(InvalidArgumentException::class);
    $queue = $this->fixture->queue(['queue' => str_repeat('é', 64)]);
    expect(fn () => $queue->publisher()->publish($this->fixture->message(str_repeat('é', 128))))->toThrow(InvalidMessageException::class);
    foreach (['A', 'a', 'é', "e\u{0301}", str_repeat('é', 127) . 'a'] as $id) {
        $queue->publisher()->publish($this->fixture->message($id));
    }
    $receiver = $queue->receiver();
    $ids = [];
    while (($delivery = $receiver->receive()) !== null) {
        $ids[] = $this->fixture->codec->decode($delivery->raw())->id;
        $delivery->acknowledge();
    }
    expect($ids)->toHaveCount(5)->and(count(array_unique($ids)))->toBe(5);
});

it('compares original header values while ignoring their insertion order', function (): void {
    $queue = $this->fixture->queue();
    $base = $this->fixture->message();
    $first = new Message($base->id, $base->type, $base->body, new Headers(['a' => 'one', 'b' => 'two']));
    $reordered = new Message($base->id, $base->type, $base->body, new Headers(['b' => 'two', 'a' => 'one']));
    $different = new Message($base->id, $base->type, $base->body, new Headers(['a' => 'one', 'b' => 'changed']));
    $queue->publisher()->publish($first);
    $queue->publisher()->publish($reordered);
    expect(fn () => $queue->publisher()->publish($different))->toThrow(MessageConflictException::class);
    expect((int) $this->fixture->row('SELECT count(*) AS count FROM ' . $this->fixture->table())['count'])->toBe(1);
});

it('rejects unsupported durations before any database change', function (): void {
    $queue = $this->fixture->queue();
    expect(fn () => $queue->publisher()->publish($this->fixture->message(), QueueConfig::MAX_DURATION_MILLISECONDS + 1))->toThrow(UnsupportedFeatureException::class);
    expect((int) $this->fixture->row('SELECT count(*) AS count FROM ' . $this->fixture->table())['count'])->toBe(0);
    $queue->publisher()->publish($this->fixture->message(), 25);
    $receiver = $queue->receiver();
    $available = new DateTimeImmutable($this->fixture->row('SELECT available_at FROM ' . $this->fixture->table())['available_at']);
    $delivery = $receiver->receive(1_000);
    expect($delivery)->not->toBeNull()->and($delivery->metadata()->receivedAt)->toBeGreaterThanOrEqual($available);
});

it('binds cursors to one installation and preserves them across ordinary new connections', function (): void {
    $queue = $this->fixture->queue();
    foreach (['old-1', 'old-2'] as $id) {
        $queue->publisher()->publish($this->fixture->message($id));
        $queue->receiver()->receive()->fail(new FailureDetails(FailureReason::Handler));
    }
    $cursor = $queue->failures()->page(new FailureQuery(1))->nextCursor;
    expect($cursor)->not->toBeNull();
    $same = $this->fixture->queue(database: $this->fixture->newDatabase());
    expect($same->failures()->page(new FailureQuery(1, $cursor))->items[0]->messageId)->toBe('old-2');

    $this->fixture->execute('DROP TABLE ' . $this->fixture->table('queue_failures'));
    $this->fixture->execute('DROP TABLE ' . $this->fixture->table());
    Schema::install($this->fixture->database);
    foreach (['replacement-1', 'replacement-2'] as $id) {
        $queue->publisher()->publish($this->fixture->message($id));
        $queue->receiver()->receive()->fail(new FailureDetails(FailureReason::Handler));
    }
    expect(fn () => $queue->failures()->page(new FailureQuery(1, $cursor)))->toThrow(InvalidArgumentException::class);
    expect($queue->failures()->page(new FailureQuery(1))->items[0]->messageId)->toBe('replacement-1');
});

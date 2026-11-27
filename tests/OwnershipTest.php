<?php

declare(strict_types=1);

use Componenta\Queue\Cycle\Tests\Support\Process;
use Componenta\Queue\Exception\DeliveryLostException;
use Componenta\Queue\Exception\ReceiverClosedException;
use Componenta\Queue\FailureDetails;
use Componenta\Queue\FailureQuery;
use Componenta\Queue\FailureReason;
use Cycle\Database\DatabaseInterface;
use Cycle\Database\Injection\Parameter;

it('reserves the next unlocked row using another physical connection', function (): void {
    $queue = $this->fixture->queue();
    $queue->publisher()->publish($this->fixture->message('first'));
    $queue->publisher()->publish($this->fixture->message('second'));
    $writer = $this->fixture->database->getDriver(DatabaseInterface::WRITE);
    $writer->beginTransaction();
    $this->fixture->row('SELECT row_id FROM ' . $this->fixture->table() . " WHERE logical_id = 'first' FOR UPDATE");
    $other = $this->fixture->queue(database: $this->fixture->newDatabase())->receiver();
    $second = $other->receive();
    expect($this->fixture->codec->decode($second->raw())->id)->toBe('second');
    $writer->rollbackTransaction();
    $first = $queue->receiver()->receive();
    expect($this->fixture->codec->decode($first->raw())->id)->toBe('first');
    $second->acknowledge();
    $first->acknowledge();
});

it('recovers a committed reservation after its process exits before handling', function (): void {
    $queue = $this->fixture->queue();
    $queue->publisher()->publish($this->fixture->message());
    $process = new Process($this->fixture);
    expect($process->request(['op' => 'receive']))->toBe(['id' => 'message-1', 'count' => 1]);
    $process->crash();
    expect($this->fixture->row('SELECT state FROM ' . $this->fixture->table())['state'])->toBe('leased');
    expect($queue->receiver()->receive())->toBeNull();
    $this->fixture->expire('message-1');
    $delivery = $queue->receiver()->receive();
    expect($delivery->metadata()->deliveryCount)->toBe(2)->and($delivery->metadata()->retryCount)->toBe(0);
    $delivery->acknowledge();
});

it('fences an old delivery after a new token was committed', function (string $operation): void {
    $queue = $this->fixture->queue();
    $queue->publisher()->publish($this->fixture->message());
    $receiver = $queue->receiver();
    $old = $receiver->receive();
    $this->fixture->expire('message-1');
    $next = $this->fixture->queue(database: $this->fixture->newDatabase())->receiver()->receive();
    $call = match ($operation) {
        'ack' => $old->acknowledge(...),
        'retry' => fn () => $old->retry(0),
        'fail' => fn () => $old->fail(new FailureDetails(FailureReason::Handler)),
        'renew' => fn () => $old->lease()->renew(1_000),
    };
    expect($call)->toThrow(DeliveryLostException::class);
    expect(fn () => $receiver->receive())->toThrow(ReceiverClosedException::class);
    expect($next->metadata()->deliveryCount)->toBe(2);
    $next->acknowledge();
})->with(['ack', 'retry', 'fail', 'renew']);

it('checks expiry using fresh time after waiting for a row lock', function (): void {
    $process = new Process($this->fixture);
    $queue = $this->fixture->queue(['leaseMilliseconds' => 300]);
    $queue->publisher()->publish($this->fixture->message());
    $delivery = $queue->receiver()->receive();
    $process->send(['op' => 'hold_until_expired', 'id' => 'message-1']);
    expect($process->read())->toBe(['locked' => true]);
    expect(fn () => $delivery->acknowledge())->toThrow(DeliveryLostException::class);
    expect($process->read())->toBe(['released' => true]);
    expect($queue->receiver()->receive()->metadata()->deliveryCount)->toBe(2);
});

it('replaces lease duration from database time and cannot renew after expiry', function (): void {
    $queue = $this->fixture->queue(['leaseMilliseconds' => 5_000]);
    $queue->publisher()->publish($this->fixture->message());
    $delivery = $queue->receiver()->receive();
    $snapshot = $delivery->lease()->renew(500);
    $row = $this->fixture->row('SELECT EXTRACT(epoch FROM (lease_until - clock_timestamp())) AS remaining, retry_count, delivery_count FROM ' . $this->fixture->table());
    expect((float) $row['remaining'])->toBeGreaterThan(0)->toBeLessThanOrEqual(0.5)
        ->and((int) $row['retry_count'])->toBe(0)->and((int) $row['delivery_count'])->toBe(1)
        ->and($snapshot->expiresAt)->toBeInstanceOf(DateTimeImmutable::class);
    $this->fixture->expire('message-1');
    expect(fn () => $delivery->lease()->renew(5_000))->toThrow(DeliveryLostException::class);
});

it('quarantines exhausted reservations in bounded polls before reaching ready work', function (): void {
    $queue = $this->fixture->queue(['maxDeliveries' => 1, 'maintenanceLimit' => 1]);
    foreach (range(1, 4) as $number) {
        $id = 'crashed-' . $number;
        $queue->publisher()->publish($this->fixture->message($id));
        $receiver = $queue->receiver();
        expect($this->fixture->codec->decode($receiver->receive()->raw())->id)->toBe($id);
        $receiver->close();
    }
    $this->fixture->execute('UPDATE ' . $this->fixture->table() . " SET lease_until = clock_timestamp() WHERE state = 'leased'");
    $queue->publisher()->publish($this->fixture->message('healthy'));
    $receiver = $queue->receiver();
    foreach (range(1, 4) as $count) {
        expect($receiver->receive())->toBeNull();
        $page = $queue->failures()->page(new FailureQuery(10));
        expect($page->items)->toHaveCount($count)
            ->and($page->items[$count - 1]->reason)->toBe(FailureReason::DeliveryLimit);
    }
    expect($this->fixture->codec->decode($receiver->receive()->raw())->id)->toBe('healthy');
});

it('closes a direct receiver if snapshot construction fails after its claim committed', function (): void {
    $queue = $this->fixture->queue();
    $message = $this->fixture->message();
    $queue->publisher()->publish($message);
    $table = $this->fixture->table();
    $this->fixture->execute("UPDATE $table SET raw_headers = :headers", ['headers' => new Parameter('broken header storage', PDO::PARAM_LOB)]);
    $receiver = $queue->receiver();
    expect(fn () => $receiver->receive())->toThrow(JsonException::class);
    expect(fn () => $receiver->receive())->toThrow(ReceiverClosedException::class);
    expect($this->fixture->row("SELECT state, delivery_count FROM $table"))->toBe(['state' => 'leased', 'delivery_count' => 1]);
    expect($queue->failures()->page(new FailureQuery(10))->items)->toBeEmpty();

    $headers = json_encode((object) $this->fixture->codec->encode($message)->headers->values, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
    $this->fixture->execute("UPDATE $table SET raw_headers = :headers", ['headers' => new Parameter($headers, PDO::PARAM_LOB)]);
    $this->fixture->expire($message->id);
    $recovered = $queue->receiver()->receive();
    expect($recovered->metadata()->deliveryCount)->toBe(2)
        ->and($this->fixture->codec->decode($recovered->raw())->body)->toBe($message->body);
    $recovered->acknowledge();
});

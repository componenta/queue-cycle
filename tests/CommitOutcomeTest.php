<?php

declare(strict_types=1);

use Componenta\Queue\Cycle\Tests\Support\CommitProxy;
use Componenta\Queue\Exception\IndeterminateOperationException;
use Componenta\Queue\Exception\ReceiverClosedException;
use Componenta\Queue\Handler\CallbackHandler;
use Componenta\Queue\OperationOutcome;
use Componenta\Queue\Retry\NoRetryPolicy;
use Componenta\Queue\Worker;

it('completes real transactions through an unfaulted TCP protocol relay', function (): void {
    $proxy = new CommitProxy('pass');
    $queue = $this->fixture->queue(database: $this->fixture->newDatabase($proxy->dsn));
    $queue->publisher()->publish($this->fixture->message());
    $receiver = $queue->receiver();
    $receiver->receive()->acknowledge();
    expect($receiver->receive())->toBeNull();
});

it('reports publication uncertainty on both sides of a real commit boundary', function (string $boundary, int $durableRows): void {
    $proxy = new CommitProxy($boundary);
    $queue = $this->fixture->queue(database: $this->fixture->newDatabase($proxy->dsn));
    try {
        $queue->publisher()->publish($this->fixture->message());
        $this->fail('The relay must interrupt the COMMIT response.');
    } catch (IndeterminateOperationException $error) {
        expect($error->outcome)->toBe(OperationOutcome::Indeterminate)->and($error->getPrevious())->not->toBeNull();
    }
    expect($proxy->read())->toBe(['boundary' => $boundary]);
    expect((int) $this->fixture->row('SELECT count(*) AS count FROM ' . $this->fixture->table())['count'])->toBe($durableRows);

    // The same logical publication is an explicit safe recovery action in this profile.
    $recovery = $this->fixture->queue();
    $recovery->publisher()->publish($this->fixture->message());
    $receiver = $recovery->receiver();
    $receiver->receive()->acknowledge();
    expect($receiver->receive())->toBeNull();
})->with([['before', 0], ['after', 1]]);

it('never invokes a handler when a committed reservation response was lost', function (): void {
    $direct = $this->fixture->queue();
    $direct->publisher()->publish($this->fixture->message());
    $proxy = new CommitProxy('after');
    $receiver = $this->fixture->queue(database: $this->fixture->newDatabase($proxy->dsn))->receiver();
    $calls = 0;
    $worker = new Worker($receiver, $this->fixture->codec, new CallbackHandler(function () use (&$calls): void {
        ++$calls;
    }), new NoRetryPolicy());
    expect(fn () => $worker->processOne())->toThrow(IndeterminateOperationException::class);
    expect($proxy->read())->toBe(['boundary' => 'after']);
    expect($calls)->toBe(0);
    expect(fn () => $receiver->receive())->toThrow(ReceiverClosedException::class);
    $row = $this->fixture->row('SELECT state, delivery_count FROM ' . $this->fixture->table());
    expect($row['state'])->toBe('leased')->and((int) $row['delivery_count'])->toBe(1);
    $this->fixture->expire('message-1');
    $delivery = $direct->receiver()->receive();
    expect($delivery->metadata()->deliveryCount)->toBe(2);
    $delivery->acknowledge();
});

it('invalidates settlement after a lost commit response without applying any opposite disposition', function (): void {
    $direct = $this->fixture->queue();
    $direct->publisher()->publish($this->fixture->message());
    $proxy = new CommitProxy('after', 2);
    $receiver = $this->fixture->queue(database: $this->fixture->newDatabase($proxy->dsn))->receiver();
    $delivery = $receiver->receive();
    expect(fn () => $delivery->acknowledge())->toThrow(IndeterminateOperationException::class);
    expect($proxy->read())->toBe(['boundary' => 'after']);
    expect(fn () => $delivery->retry(0))->toThrow(IndeterminateOperationException::class);
    expect(fn () => $receiver->receive())->toThrow(ReceiverClosedException::class);
    expect($this->fixture->row('SELECT state, retry_count FROM ' . $this->fixture->table()))
        ->toBe(['state' => 'completed', 'retry_count' => 0]);
    expect($direct->receiver()->receive())->toBeNull();
});

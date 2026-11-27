<?php

declare(strict_types=1);

use Componenta\Queue\Cycle\Tests\Support\Process;
use Componenta\Queue\Exception\FailureStateConflictException;
use Componenta\Queue\Exception\MessageConflictException;
use Componenta\Queue\FailureDetails;
use Componenta\Queue\FailureQuery;
use Componenta\Queue\FailureReason;
use Cycle\Database\DatabaseInterface;

it('arbitrates concurrent publications by full original content including delay', function (bool $conflict): void {
    $process = new Process($this->fixture);
    $queue = $this->fixture->queue();
    $writer = $this->fixture->database->getDriver(DatabaseInterface::WRITE);
    $writer->beginTransaction();
    $queue->transactionalPublisher()->stage($this->fixture->message());
    $process->send(['op' => 'publish', 'id' => 'message-1', 'delay' => $conflict ? 1 : 0]);
    $process->awaitLock($this->fixture);
    $writer->commitTransaction();

    $result = $process->read();
    if ($conflict) {
        expect($result['error'])->toBe(MessageConflictException::class);
    } else {
        expect($result)->toBe(['ok' => true]);
    }
    expect((int) $this->fixture->row('SELECT count(*) AS count FROM ' . $this->fixture->table())['count'])->toBe(1);
    $receiver = $queue->receiver();
    $receiver->receive()->acknowledge();
    expect($receiver->receive())->toBeNull();
})->with([false, true]);

it('serializes concurrent same-parameter redrive into one processing generation', function (): void {
    $queue = $this->fixture->queue();
    $queue->publisher()->publish($this->fixture->message());
    $queue->receiver()->receive()->fail(new FailureDetails(FailureReason::Handler));
    $id = $queue->failures()->page(new FailureQuery(1))->items[0]->failureId;
    $first = new Process($this->fixture);
    $second = new Process($this->fixture);
    $writer = $this->fixture->database->getDriver(DatabaseInterface::WRITE);
    $writer->beginTransaction();
    $this->fixture->row('SELECT row_id FROM ' . $this->fixture->table() . ' FOR UPDATE');
    $first->send(['op' => 'redrive', 'id' => $id]);
    $second->send(['op' => 'redrive', 'id' => $id]);
    $first->awaitLock($this->fixture);
    $second->awaitLock($this->fixture);
    $writer->commitTransaction();
    expect($first->read())->toBe(['ok' => true])->and($second->read())->toBe(['ok' => true]);
    expect($this->fixture->row('SELECT generation, redrive_count, delivery_count FROM ' . $this->fixture->table()))
        ->toBe(['generation' => 2, 'redrive_count' => 1, 'delivery_count' => 0]);
    $receiver = $queue->receiver();
    $receiver->receive()->acknowledge();
    expect($receiver->receive())->toBeNull();
});

it('makes concurrent conflicting administration choose one durable outcome', function (string $secondOperation, int $secondDelay): void {
    $queue = $this->fixture->queue();
    $queue->publisher()->publish($this->fixture->message());
    $queue->receiver()->receive()->fail(new FailureDetails(FailureReason::Handler));
    $id = $queue->failures()->page(new FailureQuery(1))->items[0]->failureId;
    $first = new Process($this->fixture);
    $second = new Process($this->fixture);
    $writer = $this->fixture->database->getDriver(DatabaseInterface::WRITE);
    $writer->beginTransaction();
    $this->fixture->row('SELECT row_id FROM ' . $this->fixture->table() . ' FOR UPDATE');
    $first->send(['op' => 'redrive', 'id' => $id, 'delay' => 0]);
    $second->send(['op' => $secondOperation, 'id' => $id, 'delay' => $secondDelay]);
    $first->awaitLock($this->fixture);
    $second->awaitLock($this->fixture);
    $writer->commitTransaction();
    $results = [$first->read(), $second->read()];
    expect(count(array_filter($results, static fn (array $result): bool => ($result['ok'] ?? false) === true)))->toBe(1);
    $errors = array_values(array_filter($results, static fn (array $result): bool => isset($result['error'])));
    expect($errors[0]['error'])->toBe(FailureStateConflictException::class);
    $row = $this->fixture->row('SELECT state, generation FROM ' . $this->fixture->table());
    expect($row['state'])->toBeIn(['ready', 'discarded'])
        ->and((int) $row['generation'])->toBe($row['state'] === 'ready' ? 2 : 1);
})->with([['redrive', 10], ['discard', 0]]);

it('skips a locked cleanup target while a concurrent duplicate publication retains its identity', function (): void {
    $queue = $this->fixture->queue(['dedupRetentionMilliseconds' => 0]);
    $queue->publisher()->publish($this->fixture->message());
    $queue->receiver()->receive()->acknowledge();
    $publish = new Process($this->fixture);
    $cleanup = new Process($this->fixture, ['dedupRetentionMilliseconds' => 0]);
    $writer = $this->fixture->database->getDriver(DatabaseInterface::WRITE);
    $writer->beginTransaction();
    $this->fixture->row('SELECT row_id FROM ' . $this->fixture->table() . ' FOR UPDATE');
    $publish->send(['op' => 'publish', 'id' => 'message-1']);
    $publish->awaitLock($this->fixture);
    expect($cleanup->request(['op' => 'cleanup']))->toBe(['messages' => 0, 'failures' => 0, 'detached' => 0]);
    $writer->commitTransaction();
    expect($publish->read())->toBe(['ok' => true]);
    expect($queue->receiver()->receive())->toBeNull();
    expect($queue->cleanup(10)->messagesDeleted)->toBe(1);
    $queue->publisher()->publish($this->fixture->message());
    expect($queue->receiver()->receive()->metadata()->deliveryCount)->toBe(1);
});

it('preserves pending failure identity while cleanup races a blocked redrive', function (): void {
    $queue = $this->fixture->queue(['dedupRetentionMilliseconds' => 0, 'failureRetentionMilliseconds' => 0]);
    $queue->publisher()->publish($this->fixture->message());
    $queue->receiver()->receive()->fail(new FailureDetails(FailureReason::Handler));
    $id = $queue->failures()->page(new FailureQuery(1))->items[0]->failureId;
    $redrive = new Process($this->fixture);
    $cleanup = new Process($this->fixture, ['dedupRetentionMilliseconds' => 0, 'failureRetentionMilliseconds' => 0]);
    $writer = $this->fixture->database->getDriver(DatabaseInterface::WRITE);
    $writer->beginTransaction();
    $this->fixture->row('SELECT row_id FROM ' . $this->fixture->table() . ' FOR UPDATE');
    $redrive->send(['op' => 'redrive', 'id' => $id]);
    $redrive->awaitLock($this->fixture);
    expect($cleanup->request(['op' => 'cleanup']))->toBe(['messages' => 0, 'failures' => 0, 'detached' => 0]);
    $writer->commitTransaction();
    expect($redrive->read())->toBe(['ok' => true]);
    expect($queue->receiver()->receive()->metadata()->deliveryCount)->toBe(1);
});

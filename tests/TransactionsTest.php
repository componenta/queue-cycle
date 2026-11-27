<?php

declare(strict_types=1);

use Componenta\Queue\Exception\IndeterminateOperationException;
use Componenta\Queue\Exception\TransportException;
use Componenta\Queue\FailureDetails;
use Componenta\Queue\FailureQuery;
use Componenta\Queue\FailureReason;
use Componenta\Queue\OperationOutcome;
use Cycle\Database\DatabaseInterface;

it('stages on the caller write transaction without changing settings or exposing uncommitted work', function (): void {
    $writer = $this->fixture->database->getDriver(DatabaseInterface::WRITE);
    $queue = $this->fixture->queue();
    $other = $this->fixture->queue(database: $this->fixture->newDatabase());
    $writer->beginTransaction();
    $writer->execute("SET LOCAL statement_timeout = '1234ms'");
    $queue->transactionalPublisher()->stage($this->fixture->message());
    expect($writer->getTransactionLevel())->toBe(1)
        ->and($this->fixture->row("SELECT current_setting('statement_timeout') AS value")['value'])->toBe('1234ms')
        ->and($other->receiver()->receive())->toBeNull();
    $writer->rollbackTransaction();
    expect($other->receiver()->receive())->toBeNull();

    $writer->beginTransaction();
    $queue->transactionalPublisher()->stage($this->fixture->message());
    $writer->commitTransaction();
    $delivery = $other->receiver()->receive();
    expect($delivery)->not->toBeNull();
    $delivery->acknowledge();
});

it('anchors staged delay before a long caller transaction commits', function (): void {
    $writer = $this->fixture->database->getDriver(DatabaseInterface::WRITE);
    $queue = $this->fixture->queue();
    $other = $this->fixture->queue(database: $this->fixture->newDatabase());
    $writer->beginTransaction();
    $queue->transactionalPublisher()->stage($this->fixture->message(), 30);
    $deadline = hrtime(true) + 1_000_000_000;
    while (!$this->fixture->row('SELECT available_at <= clock_timestamp() AS elapsed FROM ' . $this->fixture->table())['elapsed']) {
        if (hrtime(true) > $deadline) {
            throw new RuntimeException('The staging delay did not elapse.');
        }
        usleep(1_000);
    }
    expect($other->receiver()->receive())->toBeNull();
    $writer->commitTransaction();
    expect($other->receiver()->receive())->not->toBeNull();
});

it('rejects ordinary operations inside application transactions and savepoints before changing them', function (): void {
    $writer = $this->fixture->database->getDriver(DatabaseInterface::WRITE);
    $queue = $this->fixture->queue();
    $queue->publisher()->publish($this->fixture->message());
    $receiver = $queue->receiver();
    $delivery = $receiver->receive();
    $writer->beginTransaction();
    $writer->beginTransaction();

    expect(fn () => $queue->publisher()->publish($this->fixture->message('other')))->toThrow(TransportException::class);
    expect(fn () => $queue->receiver()->receive())->toThrow(TransportException::class);
    expect(fn () => $delivery->acknowledge())->toThrow(TransportException::class);
    expect($writer->getTransactionLevel())->toBe(2);
    $writer->rollbackTransaction();
    $writer->rollbackTransaction();
    $delivery->acknowledge();
    expect($receiver->receive())->toBeNull();
});

it('keeps caller rollback responsibility when transactional staging SQL fails', function (): void {
    $table = $this->fixture->table();
    $this->fixture->execute("ALTER TABLE $table ADD CONSTRAINT reject_stage CHECK (logical_id <> 'bad')");
    $writer = $this->fixture->database->getDriver(DatabaseInterface::WRITE);
    $writer->beginTransaction();
    $queue = $this->fixture->queue();
    expect(fn () => $queue->transactionalPublisher()->stage($this->fixture->message('bad')))
        ->toThrow(IndeterminateOperationException::class);
    expect($writer->getTransactionLevel())->toBe(1);
    $writer->rollbackTransaction();
    $queue->publisher()->publish($this->fixture->message('good'));
    expect($queue->receiver()->receive())->not->toBeNull();
});

it('rolls back a failed reservation before any durable delivery count can advance', function (): void {
    $queue = $this->fixture->queue();
    $queue->publisher()->publish($this->fixture->message());
    $table = $this->fixture->table();
    $this->fixture->execute("ALTER TABLE $table ADD CONSTRAINT reject_claim CHECK (delivery_count = 0)");
    $receiver = $queue->receiver();
    try {
        $receiver->receive();
        $this->fail('Claim should have failed at the database constraint.');
    } catch (TransportException $error) {
        expect($error->outcome)->toBe(OperationOutcome::NotApplied);
    }
    expect((int) $this->fixture->row("SELECT delivery_count FROM $table")['delivery_count'])->toBe(0);
    $this->fixture->execute("ALTER TABLE $table DROP CONSTRAINT reject_claim");
    expect($receiver->receive()->metadata()->deliveryCount)->toBe(1);
});

it('rolls back retry availability and counters as one transition and permits an explicit later attempt', function (): void {
    $queue = $this->fixture->queue();
    $queue->publisher()->publish($this->fixture->message());
    $receiver = $queue->receiver();
    $delivery = $receiver->receive();
    $table = $this->fixture->table();
    $this->fixture->execute("ALTER TABLE $table ADD CONSTRAINT reject_retry CHECK (retry_count = 0)");
    try {
        $delivery->retry(1_000);
        $this->fail('Retry should have failed at the database constraint.');
    } catch (TransportException $error) {
        expect($error->outcome)->toBe(OperationOutcome::NotApplied);
    }
    $row = $this->fixture->row("SELECT state, retry_count, delivery_count, lease_token FROM $table");
    expect($row['state'])->toBe('leased')->and((int) $row['retry_count'])->toBe(0)
        ->and((int) $row['delivery_count'])->toBe(1)->and($row['lease_token'])->not->toBeNull();
    $this->fixture->execute("ALTER TABLE $table DROP CONSTRAINT reject_retry");
    $delivery->retry(0);
    expect($receiver->receive()->metadata()->retryCount)->toBe(1);
});

it('keeps the source reservation recoverable when atomic failure insertion is rejected', function (): void {
    $queue = $this->fixture->queue();
    $queue->publisher()->publish($this->fixture->message());
    $receiver = $queue->receiver();
    $delivery = $receiver->receive();
    $failures = $this->fixture->table('queue_failures');
    $this->fixture->execute("ALTER TABLE $failures ADD CONSTRAINT reject_failure CHECK (false) NOT VALID");
    try {
        $delivery->fail(new FailureDetails(FailureReason::Handler));
        $this->fail('Failure copy should have been rejected.');
    } catch (TransportException $error) {
        expect($error->outcome)->toBe(OperationOutcome::NotApplied);
    }
    expect($this->fixture->row('SELECT state FROM ' . $this->fixture->table())['state'])->toBe('leased')
        ->and($queue->failures()->page(new FailureQuery(10))->items)->toBeEmpty();
    $this->fixture->execute("ALTER TABLE $failures DROP CONSTRAINT reject_failure");
    $delivery->fail(new FailureDetails(FailureReason::Handler));
    expect($queue->failures()->page(new FailureQuery(10))->items)->toHaveCount(1)
        ->and($receiver->receive())->toBeNull();
});

it('rolls back an inserted failure snapshot if changing the source state fails', function (): void {
    $queue = $this->fixture->queue();
    $queue->publisher()->publish($this->fixture->message());
    $delivery = $queue->receiver()->receive();
    $table = $this->fixture->table();
    $this->fixture->execute("ALTER TABLE $table ADD CONSTRAINT reject_failed_state CHECK (state <> 'failed')");
    expect(fn () => $delivery->fail(new FailureDetails(FailureReason::Handler)))->toThrow(TransportException::class);
    expect($queue->failures()->page(new FailureQuery(10))->items)->toBeEmpty()
        ->and($this->fixture->row("SELECT state FROM $table")['state'])->toBe('leased');
    $this->fixture->execute("ALTER TABLE $table DROP CONSTRAINT reject_failed_state");
    $delivery->fail(new FailureDetails(FailureReason::Handler));
    expect($queue->failures()->page(new FailureQuery(10))->items)->toHaveCount(1);
});

it('rolls back a new generation when resolving its failure record fails', function (): void {
    $queue = $this->fixture->queue();
    $queue->publisher()->publish($this->fixture->message());
    $queue->receiver()->receive()->fail(new FailureDetails(FailureReason::Handler));
    $id = $queue->failures()->page(new FailureQuery(1))->items[0]->failureId;
    $failures = $this->fixture->table('queue_failures');
    $this->fixture->execute("ALTER TABLE $failures ADD CONSTRAINT reject_redrive CHECK (status <> 'redriven')");
    expect(fn () => $queue->failures()->redrive($id))->toThrow(TransportException::class);
    expect($this->fixture->row('SELECT state, generation, redrive_count FROM ' . $this->fixture->table()))
        ->toBe(['state' => 'failed', 'generation' => 1, 'redrive_count' => 0]);
    expect($queue->receiver()->receive())->toBeNull();
    $this->fixture->execute("ALTER TABLE $failures DROP CONSTRAINT reject_redrive");
    $queue->failures()->redrive($id);
    expect($queue->receiver()->receive()->metadata()->deliveryCount)->toBe(1);
});

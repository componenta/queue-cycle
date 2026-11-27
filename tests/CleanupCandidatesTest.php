<?php

declare(strict_types=1);

use Componenta\Queue\FailureDetails;
use Componenta\Queue\FailureQuery;
use Componenta\Queue\FailureReason;
use Componenta\Queue\FailureStatus;

it('counts a ledger due through both terminal and history retention only once', function (): void {
    $queue = $this->fixture->queue(['dedupRetentionMilliseconds' => 0, 'failureRetentionMilliseconds' => 0]);
    $queue->publisher()->publish($this->fixture->message());
    $queue->receiver()->receive()->fail(new FailureDetails(FailureReason::Handler));
    $failureId = $queue->failures()->page(new FailureQuery(1))->items[0]->failureId;
    $queue->failures()->discard($failureId);

    $result = $queue->cleanup(3);

    expect($result->messagesDeleted)->toBe(1)
        ->and($result->failuresDeleted)->toBe(1)
        ->and($result->historiesDetached)->toBe(0)
        ->and($queue->failures()->find($failureId))->toBeNull();
    $repeat = $queue->cleanup(3);
    expect($repeat->messagesDeleted + $repeat->failuresDeleted + $repeat->historiesDetached)->toBe(0);
});

it('keeps due history and active work in a neighbouring queue outside cleanup', function (): void {
    $queues = [];
    $ids = [];
    foreach (['orders', 'Orders'] as $name) {
        $queue = $this->fixture->queue(['queue' => $name, 'dedupRetentionMilliseconds' => 0, 'failureRetentionMilliseconds' => 0]);
        $queues[] = $queue;
        $queue->publisher()->publish($this->fixture->message('same-logical-id'));
        $queue->receiver()->receive()->fail(new FailureDetails(FailureReason::Handler));
        $ids[] = $queue->failures()->page(new FailureQuery(1))->items[0]->failureId;
    }
    [$target, $other] = $queues;
    $target->failures()->discard($ids[0]);
    $other->failures()->redrive($ids[1]);
    $active = $other->receiver()->receive();

    $result = $target->cleanup(10);

    expect($result->messagesDeleted)->toBe(1)
        ->and($result->failuresDeleted)->toBe(1)
        ->and($result->historiesDetached)->toBe(0)
        ->and($other->failures()->find($ids[1])?->summary->status)->toBe(FailureStatus::Redriven);
    $active->acknowledge();
    $otherResult = $other->cleanup(10);
    expect($otherResult->messagesDeleted)->toBe(1)
        ->and($otherResult->failuresDeleted)->toBe(1);
});

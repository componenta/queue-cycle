<?php

declare(strict_types=1);

use Componenta\Queue\Exception\FailureStateConflictException;
use Componenta\Queue\FailureDetails;
use Componenta\Queue\FailureQuery;
use Componenta\Queue\FailureReason;

it('removes expired resolved history while its newer processing generation remains active', function (): void {
    $queue = $this->fixture->queue(['failureRetentionMilliseconds' => 0, 'dedupRetentionMilliseconds' => null]);
    $queue->publisher()->publish($this->fixture->message('older'));
    $receiver = $queue->receiver();
    $receiver->receive()->fail(new FailureDetails(FailureReason::Handler));
    $first = $queue->failures()->page(new FailureQuery(1))->items[0]->failureId;
    $queue->failures()->redrive($first);
    $active = $receiver->receive();

    $queue->publisher()->publish($this->fixture->message('later'));
    $receiver = $queue->receiver();
    $receiver->receive()->fail(new FailureDetails(FailureReason::Handler));
    $second = $queue->failures()->page(new FailureQuery(2))->items[1]->failureId;
    $queue->failures()->discard($second);

    expect($queue->cleanup(1)->failuresDeleted)->toBe(1);
    expect($queue->cleanup(1)->failuresDeleted)->toBe(1);
    expect($queue->failures()->find($first))->toBeNull()
        ->and($queue->failures()->find($second))->toBeNull();
    $active->acknowledge();
});

it('detaches retained history before allowing a new incarnation of a cleaned logical ID', function (): void {
    $queue = $this->fixture->queue(['failureRetentionMilliseconds' => null, 'dedupRetentionMilliseconds' => 0]);
    $message = $this->fixture->message();
    $queue->publisher()->publish($message);
    $receiver = $queue->receiver();
    $receiver->receive()->fail(new FailureDetails(FailureReason::Handler));
    $id = $queue->failures()->page(new FailureQuery(1))->items[0]->failureId;
    $queue->failures()->discard($id);

    expect($queue->cleanup(1)->historiesDetached)->toBe(1);
    expect($queue->cleanup(1)->messagesDeleted)->toBe(1);
    $queue->publisher()->publish($this->fixture->message(body: 'new incarnation'));
    $queue->failures()->discard($id);
    expect(fn () => $queue->failures()->redrive($id))->toThrow(FailureStateConflictException::class);
    expect($this->fixture->codec->decode($receiver->receive()->raw())->body)->toBe('new incarnation');
});

it('never purges pending failed or live work through terminal retention', function (): void {
    $queue = $this->fixture->queue(['failureRetentionMilliseconds' => 0, 'dedupRetentionMilliseconds' => 0]);
    $queue->publisher()->publish($this->fixture->message('pending'));
    $receiver = $queue->receiver();
    $receiver->receive()->fail(new FailureDetails(FailureReason::Handler));
    $id = $queue->failures()->page(new FailureQuery(1))->items[0]->failureId;
    $queue->publisher()->publish($this->fixture->message('live'));

    $result = $queue->cleanup(100);
    expect($result->messagesDeleted)->toBe(0)->and($result->failuresDeleted)->toBe(0)
        ->and($queue->failures()->find($id))->not->toBeNull()
        ->and($this->fixture->codec->decode($receiver->receive()->raw())->id)->toBe('live');
});

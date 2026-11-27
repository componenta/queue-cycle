<?php

declare(strict_types=1);

use Componenta\Queue\Exception\MessageConflictException;
use Componenta\Queue\FailureDetails;
use Componenta\Queue\FailureQuery;
use Componenta\Queue\FailureReason;
use Componenta\Queue\FailureStatus;

it('preserves binary payload and deduplicates an acknowledged logical publication', function (): void {
    $queue = $this->fixture->queue();
    $message = $this->fixture->message();
    $queue->publisher()->publish($message);
    $receiver = $queue->receiver();

    $delivery = $receiver->receive();
    expect($delivery)->not->toBeNull();
    expect($this->fixture->codec->decode($delivery->raw())->body)->toBe($message->body)
        ->and($delivery->metadata()->deliveryCount)->toBe(1)
        ->and($delivery->metadata()->retryCount)->toBe(0);
    $delivery->acknowledge();
    $queue->publisher()->publish($message);

    expect($receiver->receive())->toBeNull();
    expect(fn () => $queue->publisher()->publish($this->fixture->message(body: 'changed')))
        ->toThrow(MessageConflictException::class);
});

it('schedules retries without replacing the original bytes or original publication identity', function (): void {
    $queue = $this->fixture->queue();
    $queue->publisher()->publish($this->fixture->message());
    $receiver = $queue->receiver();
    $first = $receiver->receive();
    $raw = $first->raw();
    $first->retry(0);

    $second = $receiver->receive();
    expect($second->raw()->body)->toBe($raw->body)
        ->and($second->metadata()->retryCount)->toBe(1)
        ->and($second->metadata()->deliveryCount)->toBe(2);
    $second->acknowledge();
});

it('redrives a failed message once and preserves the resolved history', function (): void {
    $queue = $this->fixture->queue();
    $queue->publisher()->publish($this->fixture->message());
    $receiver = $queue->receiver();
    $delivery = $receiver->receive();
    $delivery->fail(new FailureDetails(FailureReason::Handler));
    $page = $queue->failures()->page(new FailureQuery(10));
    expect($page->items)->toHaveCount(1);
    $failureId = $page->items[0]->failureId;

    $queue->failures()->redrive($failureId);
    $queue->failures()->redrive($failureId);
    $next = $receiver->receive();
    expect($next->metadata()->deliveryCount)->toBe(1)
        ->and($next->metadata()->retryCount)->toBe(0)
        ->and($queue->failures()->find($failureId)->summary->status)->toBe(FailureStatus::Redriven);
    $next->acknowledge();
    expect($receiver->receive())->toBeNull();
});

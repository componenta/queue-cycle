<?php

declare(strict_types=1);

namespace Componenta\Queue\Cycle\Internal;

use Componenta\Queue\FailedMessage;
use Componenta\Queue\FailedMessageStoreInterface;
use Componenta\Queue\FailurePage;
use Componenta\Queue\FailureQuery;

/** @internal */
final readonly class FailedMessageStore implements FailedMessageStoreInterface
{
    public function __construct(private Store $store)
    {
    }
    public function find(string $failureId): ?FailedMessage
    {
        return $this->store->find($failureId);
    }
    public function page(FailureQuery $query): FailurePage
    {
        return $this->store->page($query);
    }
    public function redrive(string $failureId, int $delayMilliseconds = 0): void
    {
        $this->store->redrive($failureId, $delayMilliseconds);
    }
    public function discard(string $failureId): void
    {
        $this->store->discard($failureId);
    }
}

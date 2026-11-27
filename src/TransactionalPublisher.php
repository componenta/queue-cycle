<?php

declare(strict_types=1);

namespace Componenta\Queue\Cycle;

use Componenta\Queue\Cycle\Internal\Store;
use Componenta\Queue\Message;
use Componenta\Queue\MessageCodecInterface;

final readonly class TransactionalPublisher
{
    /** @internal Obtain this participant from PostgresQueue. */
    public function __construct(private Store $store, private MessageCodecInterface $codec)
    {
    }

    public function stage(Message $message, int $delayMilliseconds = 0): void
    {
        QueueConfig::duration($delayMilliseconds);
        $this->store->publish($message->id, $this->codec->encode($message), $delayMilliseconds, true);
    }
}

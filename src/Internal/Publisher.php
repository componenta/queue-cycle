<?php

declare(strict_types=1);

namespace Componenta\Queue\Cycle\Internal;

use Componenta\Queue\Cycle\QueueConfig;
use Componenta\Queue\Message;
use Componenta\Queue\MessageCodecInterface;
use Componenta\Queue\PublisherInterface;

/** @internal */
final readonly class Publisher implements PublisherInterface
{
    public function __construct(private Store $store, private MessageCodecInterface $codec)
    {
    }

    public function publish(Message $message, int $delayMilliseconds = 0): void
    {
        QueueConfig::duration($delayMilliseconds);
        $this->store->publish($message->id, $this->codec->encode($message), $delayMilliseconds);
    }
}

<?php

declare(strict_types=1);

namespace Componenta\Queue\Cycle;

use Componenta\Queue\Cycle\Internal\Connection;
use Componenta\Queue\Cycle\Internal\FailedMessageStore;
use Componenta\Queue\Cycle\Internal\Publisher;
use Componenta\Queue\Cycle\Internal\Receiver;
use Componenta\Queue\Cycle\Internal\Store;
use Componenta\Queue\FailedMessageStoreInterface;
use Componenta\Queue\MessageCodecInterface;
use Componenta\Queue\PublisherInterface;
use Componenta\Queue\ReceiverInterface;
use Cycle\Database\DatabaseInterface;

final readonly class PostgresQueue
{
    private Store $store;

    public function __construct(DatabaseInterface $database, QueueConfig $config, private MessageCodecInterface $codec)
    {
        $this->store = new Store(new Connection($database, $config->ioTimeoutMilliseconds), $config);
    }

    public function publisher(): PublisherInterface
    {
        return new Publisher($this->store, $this->codec);
    }

    public function receiver(): ReceiverInterface
    {
        return new Receiver($this->store);
    }

    public function failures(): FailedMessageStoreInterface
    {
        return new FailedMessageStore($this->store);
    }

    public function transactionalPublisher(): TransactionalPublisher
    {
        return new TransactionalPublisher($this->store, $this->codec);
    }

    public function cleanup(int $limit): CleanupResult
    {
        return $this->store->cleanup($limit);
    }
}

<?php

declare(strict_types=1);

namespace Componenta\Queue\Cycle\Tests;

use Componenta\Queue\Cycle\PostgresQueue;
use Componenta\Queue\Cycle\QueueConfig;
use Componenta\Queue\MessageCodecInterface;
use Componenta\Queue\Tests\Support\PortableQueueContract;
use Componenta\Queue\Tests\Support\QueueHarness;

final class PortableContractTest extends QueueTestCase
{
    use PortableQueueContract;

    protected function contractQueue(string $name = 'jobs', ?MessageCodecInterface $codec = null): QueueHarness
    {
        $codec ??= $this->fixture->codec;
        $config = new QueueConfig($name, 5_000, 10, 60_000, 60_000, 2_000, 10, 10);
        $queue = new PostgresQueue($this->fixture->database, $config, $codec);
        return new QueueHarness($queue->publisher(), $queue->receiver(...), $queue->failures(), $codec);
    }
}

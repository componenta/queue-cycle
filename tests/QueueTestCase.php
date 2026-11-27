<?php

declare(strict_types=1);

namespace Componenta\Queue\Cycle\Tests;

use PHPUnit\Framework\TestCase;

abstract class QueueTestCase extends TestCase
{
    protected Fixture $fixture;

    protected function setUp(): void
    {
        parent::setUp();
        $this->fixture = new Fixture();
    }

    protected function tearDown(): void
    {
        $this->fixture->close();
        parent::tearDown();
    }
}

<?php

declare(strict_types=1);

namespace Componenta\Queue\Cycle;

final readonly class CleanupResult
{
    public function __construct(
        public int $messagesDeleted,
        public int $failuresDeleted,
        public int $historiesDetached,
    ) {
    }
}

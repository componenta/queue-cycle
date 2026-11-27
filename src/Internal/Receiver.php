<?php

declare(strict_types=1);

namespace Componenta\Queue\Cycle\Internal;

use Componenta\Queue\Cycle\QueueConfig;
use Componenta\Queue\DeliveryInterface;
use Componenta\Queue\Exception\DeliveryStateException;
use Componenta\Queue\Exception\IndeterminateOperationException;
use Componenta\Queue\Exception\ReceiverClosedException;
use Componenta\Queue\ReceiverInterface;

/** @internal */
final class Receiver implements ReceiverInterface
{
    private bool $closed = false;
    private ?Delivery $outstanding = null;

    public function __construct(private readonly Store $store)
    {
    }

    public function receive(int $waitMilliseconds = 0): ?DeliveryInterface
    {
        if ($this->closed) {
            throw new ReceiverClosedException('This consumption session is closed.');
        }
        if ($this->outstanding !== null) {
            throw new DeliveryStateException('Settle the outstanding delivery before receiving another.');
        }
        QueueConfig::duration($waitMilliseconds);
        $start = hrtime(true);
        $budget = $waitMilliseconds * 1_000_000;
        do {
            try {
                $row = $this->store->reserve();
            } catch (IndeterminateOperationException $error) {
                $this->closed = true;
                throw $error;
            }
            if ($row !== null) {
                try {
                    return $this->outstanding = new Delivery($this->store, $row, function (bool $invalidate): void {
                        $this->outstanding = null;
                        $this->closed = $this->closed || $invalidate;
                    });
                } catch (\Throwable $error) {
                    // The reservation is already committed. Retain its expiry recovery
                    // and original construction error; never report a confirmed no-op.
                    $this->closed = true;
                    throw $error;
                }
            }
            $remaining = $budget - (hrtime(true) - $start);
            if ($remaining <= 0) {
                return null;
            }
            usleep((int) max(1, min(1_000_000, $this->store->config->pollIntervalMilliseconds * 1000, intdiv($remaining, 1000))));
        } while (true);
    }

    public function close(): void
    {
        $this->closed = true;
        $this->outstanding?->close();
        $this->outstanding = null;
        // The DatabaseInterface/driver was injected; the session does not own that connection.
    }

    /** A consumption session cannot be copied with partially shared ownership. */
    private function __clone(): void
    {
    }
}

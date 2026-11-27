<?php

declare(strict_types=1);

namespace Componenta\Queue\Cycle\Internal;

use Componenta\Queue\DeliveryInterface;
use Componenta\Queue\DeliveryMetadata;
use Componenta\Queue\Exception\DeliveryLostException;
use Componenta\Queue\Exception\DeliveryStateException;
use Componenta\Queue\Exception\IndeterminateOperationException;
use Componenta\Queue\FailureDetails;
use Componenta\Queue\LeaseInterface;
use Componenta\Queue\LeaseSnapshot;
use Componenta\Queue\RawMessage;

/** @internal */
final class Delivery implements DeliveryInterface
{
    private string $state = 'active';
    private readonly string $id;
    private readonly string $token;
    private readonly RawMessage $rawMessage;
    private readonly DeliveryMetadata $deliveryMetadata;
    private readonly Lease $leaseCapability;

    /** @param \Closure(bool):void $finished */
    public function __construct(private readonly Store $store, Row $row, private readonly \Closure $finished)
    {
        $this->id = $row->string('row_id');
        $this->token = $row->string('lease_token');
        $this->rawMessage = Store::raw($row);
        $this->deliveryMetadata = $store->metadata($row);
        $this->leaseCapability = new Lease(fn (int $milliseconds): LeaseSnapshot => $this->mutate(fn (): LeaseSnapshot => $this->store->renew($this->id, $this->token, $milliseconds), false));
    }

    public function raw(): RawMessage
    {
        return $this->rawMessage;
    }
    public function metadata(): DeliveryMetadata
    {
        return $this->deliveryMetadata;
    }
    public function lease(): LeaseInterface
    {
        return $this->leaseCapability;
    }

    public function acknowledge(): void
    {
        $this->mutate(fn () => $this->store->acknowledge($this->id, $this->token));
    }

    public function retry(int $delayMilliseconds): void
    {
        $this->mutate(fn () => $this->store->retry($this->id, $this->token, $delayMilliseconds));
    }

    public function fail(FailureDetails $failure): void
    {
        $this->mutate(fn () => $this->store->fail($this->id, $this->token, $failure));
    }

    public function close(): void
    {
        if ($this->state === 'active') {
            $this->state = 'closed';
        }
    }

    /**
     * @template T
     * @param \Closure():T $operation
     * @return T
     */
    private function mutate(\Closure $operation, bool $settles = true): mixed
    {
        if ($this->state === 'lost') {
            throw new DeliveryLostException('This delivery lost ownership.');
        }
        if ($this->state === 'indeterminate') {
            throw new IndeterminateOperationException('The previous disposition outcome is unknown.');
        }
        if ($this->state !== 'active') {
            throw new DeliveryStateException('This delivery is no longer active.');
        }
        try {
            $result = $operation();
        } catch (DeliveryLostException $error) {
            $this->state = 'lost';
            ($this->finished)(true);
            throw $error;
        } catch (IndeterminateOperationException $error) {
            $this->state = 'indeterminate';
            ($this->finished)(true);
            throw $error;
        }
        if ($settles) {
            $this->state = 'settled';
            ($this->finished)(false);
        }
        return $result;
    }
}

<?php

declare(strict_types=1);

namespace Componenta\Queue\Cycle\Internal;

use Componenta\Queue\LeaseInterface;
use Componenta\Queue\LeaseSnapshot;

/** @internal A distinct object: narrowing a Delivery object's type would still expose settlement. */
final readonly class Lease implements LeaseInterface
{
    /** @param \Closure(int):LeaseSnapshot $renew */
    public function __construct(private \Closure $renew)
    {
    }

    public function renew(int $leaseMilliseconds): LeaseSnapshot
    {
        return ($this->renew)($leaseMilliseconds);
    }
}

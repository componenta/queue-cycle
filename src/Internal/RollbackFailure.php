<?php

declare(strict_types=1);

namespace Componenta\Queue\Cycle\Internal;

/** @internal Preserves both errors when transaction cleanup itself fails. */
final class RollbackFailure extends \RuntimeException
{
    public function __construct(public readonly \Throwable $operationError, \Throwable $rollbackError)
    {
        parent::__construct('Rollback failed after a queue operation error.', 0, $rollbackError);
    }
}

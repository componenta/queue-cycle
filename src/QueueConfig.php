<?php

declare(strict_types=1);

namespace Componenta\Queue\Cycle;

use Componenta\Queue\Exception\UnsupportedFeatureException;

final readonly class QueueConfig
{
    public const int MAX_DURATION_MILLISECONDS = 9_007_199_254_740;

    public function __construct(
        public string $queue,
        public int $leaseMilliseconds,
        public int $maxDeliveries,
        public ?int $dedupRetentionMilliseconds,
        public ?int $failureRetentionMilliseconds,
        public int $ioTimeoutMilliseconds,
        public int $maintenanceLimit,
        public int $pollIntervalMilliseconds,
    ) {
        if ($queue === '' || strlen($queue) > 128 || preg_match('//u', $queue) !== 1 || preg_match('/\p{Cc}/u', $queue) !== 0) {
            throw new \InvalidArgumentException('Queue names must contain 1..128 UTF-8 bytes without control characters.');
        }
        self::duration($leaseMilliseconds, true);
        self::duration($pollIntervalMilliseconds, true);
        foreach ([$dedupRetentionMilliseconds, $failureRetentionMilliseconds] as $retention) {
            if ($retention !== null) {
                self::duration($retention);
            }
        }
        if ($maxDeliveries < 1 || $maintenanceLimit < 1 || $ioTimeoutMilliseconds < 1 || $ioTimeoutMilliseconds > 2_147_483_647) {
            throw new \InvalidArgumentException('Delivery/maintenance limits and the supported finite I/O timeout must be positive.');
        }
    }

    public static function duration(int $milliseconds, bool $positive = false): void
    {
        if ($milliseconds < ($positive ? 1 : 0)) {
            throw new \InvalidArgumentException('The duration is outside the permitted non-negative/positive range.');
        }
        if ($milliseconds > self::MAX_DURATION_MILLISECONDS) {
            throw new UnsupportedFeatureException('The duration exceeds the exact millisecond range supported by this adapter.');
        }
    }
}

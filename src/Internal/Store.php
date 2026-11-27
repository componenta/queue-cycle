<?php

declare(strict_types=1);

namespace Componenta\Queue\Cycle\Internal;

use Componenta\Queue\CountAccuracy;
use Componenta\Queue\Cycle\CleanupResult;
use Componenta\Queue\Cycle\QueueConfig;
use Componenta\Queue\DeliveryMetadata;
use Componenta\Queue\Exception\DeliveryLostException;
use Componenta\Queue\Exception\FailureNotFoundException;
use Componenta\Queue\Exception\FailureStateConflictException;
use Componenta\Queue\Exception\InvalidMessageException;
use Componenta\Queue\Exception\MessageConflictException;
use Componenta\Queue\Exception\TransportException;
use Componenta\Queue\FailedMessage;
use Componenta\Queue\FailureDetails;
use Componenta\Queue\FailurePage;
use Componenta\Queue\FailureQuery;
use Componenta\Queue\FailureReason;
use Componenta\Queue\FailureStatus;
use Componenta\Queue\FailureSummary;
use Componenta\Queue\Headers;
use Componenta\Queue\LeaseSnapshot;
use Componenta\Queue\RawMessage;
use Cycle\Database\Injection\Parameter;

/** @internal Persistence transitions share one owner, one write connection and one lock order. */
final readonly class Store
{
    public function __construct(public Connection $connection, public QueueConfig $config)
    {
    }

    public function publish(string $messageId, RawMessage $raw, int $delay, bool $staged = false): void
    {
        QueueConfig::duration($delay);
        if (strlen($messageId) > 255) {
            throw new InvalidMessageException('Logical IDs in the PostgreSQL profile are limited to 255 UTF-8 bytes.');
        }
        $headers = self::headers($raw);
        $operation = function () use ($messageId, $raw, $headers, $delay): void {
            $table = $this->ledger();
            for ($attempt = 0; $attempt < 3; ++$attempt) {
                $now = $this->connection->now();
                $inserted = $this->connection->row("INSERT INTO $table
                    (row_id, queue_name, logical_id, raw_body, raw_headers, original_delay_ms, state, generation, retry_count, delivery_count, lifetime_deliveries, redrive_count, max_deliveries, available_at, created_at)
                    VALUES (:row_id, :queue, :id, :body, :headers, :delay, 'ready', 1, 0, 0, 0, 0, :maximum, :available, :created)
                    ON CONFLICT (queue_name, logical_id) DO NOTHING RETURNING row_id", [
                    'row_id' => self::id(), 'queue' => $this->config->queue, 'id' => $messageId,
                    'body' => new Parameter($raw->body, \PDO::PARAM_LOB), 'headers' => new Parameter($headers, \PDO::PARAM_LOB),
                    'delay' => $delay, 'maximum' => $this->config->maxDeliveries,
                    'available' => Connection::format($now), 'created' => Connection::format($now),
                ]);
                if ($inserted !== null) {
                    // The unique-key arbitration may have waited. Anchor the delay after owning the inserted row.
                    $accepted = $this->connection->now();
                    $this->connection->execute("UPDATE $table SET created_at = :created, available_at = :available WHERE row_id = :row_id", [
                        'created' => Connection::format($accepted),
                        'available' => Connection::format($this->connection->after($accepted, $delay)),
                        'row_id' => $inserted->string('row_id'),
                    ]);
                    return;
                }
                $existing = $this->connection->row("SELECT raw_body, raw_headers, original_delay_ms FROM $table WHERE queue_name = :queue AND logical_id = :id FOR UPDATE", ['queue' => $this->config->queue, 'id' => $messageId]);
                if ($existing === null) {
                    // A terminal record was cleaned between unique arbitration and the row lock.
                    continue;
                }
                if ($existing->string('raw_body') !== $raw->body || $existing->string('raw_headers') !== $headers || $existing->integer('original_delay_ms') !== $delay) {
                    throw new MessageConflictException('This logical message ID is retained with different original publication content.');
                }
                return;
            }
            throw new TransportException('Concurrent publication cleanup prevented a bounded publication attempt.');
        };
        $staged ? $this->connection->stage($operation) : $this->connection->transaction($operation);
    }

    public function reserve(): ?Row
    {
        return $this->connection->transaction(function (): ?Row {
            $table = $this->ledger();
            for ($maintenance = 0; $maintenance < $this->config->maintenanceLimit; ++$maintenance) {
                // Constant range bounds let each partial index stop at its first eligible row.
                // Taking the older of both heads prevents ready traffic starving crash recovery.
                $bound = Connection::format($this->connection->now());
                $parameters = ['queue' => $this->config->queue, 'bound' => $bound];
                $ready = $this->connection->row("SELECT row_id, available_at AS eligible_at FROM $table
                    WHERE queue_name = :queue AND state = 'ready' AND available_at <= :bound
                    ORDER BY available_at, row_id LIMIT 1 FOR UPDATE SKIP LOCKED", $parameters);
                $expired = $this->connection->row("SELECT row_id, lease_until AS eligible_at FROM $table
                    WHERE queue_name = :queue AND state = 'leased' AND lease_until <= :bound
                    ORDER BY lease_until, row_id LIMIT 1 FOR UPDATE SKIP LOCKED", $parameters);
                $candidate = $ready;
                if ($candidate === null || ($expired !== null && $expired->instant('eligible_at') < $candidate->instant('eligible_at'))) {
                    $candidate = $expired;
                }
                $row = $candidate === null ? null : $this->connection->row("SELECT * FROM $table WHERE row_id = :id", ['id' => $candidate->string('row_id')]);
                if ($row === null) {
                    return null;
                }
                $now = $this->connection->now();
                $eligibleAt = $row->instant($row->string('state') === 'ready' ? 'available_at' : 'lease_until');
                if ($eligibleAt > $now) {
                    return null;
                }
                if ($row->integer('delivery_count') >= $row->integer('max_deliveries')) {
                    $this->recordFailure($row, new FailureDetails(FailureReason::DeliveryLimit), $now);
                    continue;
                }
                return $this->connection->row("UPDATE $table SET state = 'leased', lease_token = :token, lease_until = :until,
                    received_at = :received, delivery_count = delivery_count + 1, lifetime_deliveries = lifetime_deliveries + 1
                    WHERE row_id = :id RETURNING *", [
                    'token' => self::id(), 'until' => Connection::format($this->connection->after($now, $this->config->leaseMilliseconds)),
                    'received' => Connection::format($now), 'id' => $row->string('row_id'),
                ]);
            }
            return null;
        });
    }

    public function acknowledge(string $id, string $token): void
    {
        $this->connection->transaction(function () use ($id, $token): void {
            [, $now] = $this->owned($id, $token);
            $table = $this->ledger();
            $this->connection->execute("UPDATE $table SET state = 'completed', lease_token = NULL, lease_until = NULL,
                terminal_at = :now, dedup_until = :retained WHERE row_id = :id", [
                'now' => Connection::format($now), 'retained' => $this->retained($now, $this->config->dedupRetentionMilliseconds), 'id' => $id,
            ]);
        });
    }

    public function retry(string $id, string $token, int $delay): void
    {
        QueueConfig::duration($delay);
        $this->connection->transaction(function () use ($id, $token, $delay): void {
            [, $now] = $this->owned($id, $token);
            $table = $this->ledger();
            $this->connection->execute("UPDATE $table SET state = 'ready', lease_token = NULL, lease_until = NULL,
                retry_count = retry_count + 1, available_at = :available WHERE row_id = :id", [
                'available' => Connection::format($this->connection->after($now, $delay)), 'id' => $id,
            ]);
        });
    }

    public function fail(string $id, string $token, FailureDetails $details): void
    {
        $this->connection->transaction(function () use ($id, $token, $details): void {
            [$row, $now] = $this->owned($id, $token);
            $this->recordFailure($row, $details, $now);
        });
    }

    public function renew(string $id, string $token, int $milliseconds): LeaseSnapshot
    {
        QueueConfig::duration($milliseconds, true);
        return $this->connection->transaction(function () use ($id, $token, $milliseconds): LeaseSnapshot {
            [, $now] = $this->owned($id, $token);
            $until = $this->connection->after($now, $milliseconds);
            $table = $this->ledger();
            $this->connection->execute("UPDATE $table SET lease_until = :until WHERE row_id = :id", ['until' => Connection::format($until), 'id' => $id]);
            return new LeaseSnapshot($until);
        });
    }

    public function find(string $id): ?FailedMessage
    {
        return $this->connection->transaction(function () use ($id): ?FailedMessage {
            $table = $this->failures();
            $row = $this->connection->row("SELECT * FROM $table WHERE queue_name = :queue AND failure_id = :id", ['queue' => $this->config->queue, 'id' => $id]);
            return $row === null ? null : new FailedMessage($this->summary($row), self::raw($row), $this->metadata($row), new FailureDetails(FailureReason::from($row->string('reason')), $row->nullableString('exception_type'), $row->nullableString('description')));
        });
    }

    public function page(FailureQuery $query): FailurePage
    {
        if ($query->limit === PHP_INT_MAX) {
            throw new \InvalidArgumentException('The page limit must leave room for the continuation probe.');
        }
        return $this->connection->transaction(function () use ($query): FailurePage {
            $after = $this->decodeCursor($query);
            $table = $this->failures();
            $filter = $query->status === null ? '' : ' AND status = :status';
            $parameters = ['queue' => $this->config->queue, 'after' => $after, 'limit' => $query->limit + 1];
            if ($query->status !== null) {
                $parameters['status'] = $query->status->value;
            }
            $rows = $this->connection->rows("SELECT failure_id, sequence, logical_id, failed_at, reason, status FROM $table WHERE queue_name = :queue AND sequence > :after $filter ORDER BY sequence LIMIT :limit", $parameters);
            $more = count($rows) > $query->limit;
            if ($more) {
                array_pop($rows);
            }
            $last = $rows === [] ? null : $rows[array_key_last($rows)];
            $cursor = $more && $last !== null ? $this->encodeCursor($last->integer('sequence'), $query->status) : null;
            return new FailurePage(array_map($this->summary(...), $rows), $cursor);
        });
    }

    public function redrive(string $id, int $delay): void
    {
        QueueConfig::duration($delay);
        $this->connection->transaction(function () use ($id, $delay): void {
            [$ledger, $failure] = $this->administrativeRows($id);
            if ($failure->string('status') === 'redriven') {
                if ($failure->integer('redrive_delay_ms') !== $delay) {
                    throw new FailureStateConflictException('This failure was redriven with different parameters.');
                }
                return;
            }
            if ($failure->string('status') === 'discarded') {
                throw new FailureStateConflictException('A discarded failure cannot be redriven.');
            }
            $ledger = $this->pendingLedger($ledger, $failure);
            if ($ledger->string('raw_body') !== $failure->string('raw_body') || $ledger->string('raw_headers') !== $failure->string('raw_headers')) {
                throw new TransportException('The failure snapshot differs from the immutable original publication.');
            }
            $now = $this->connection->now();
            $generation = $ledger->integer('generation');
            if ($generation === PHP_INT_MAX) {
                throw new TransportException('The processing generation counter is exhausted.');
            }
            $table = $this->ledger();
            $this->connection->execute("UPDATE $table SET state = 'ready', generation = generation + 1, current_failure_id = NULL,
                retry_count = 0, delivery_count = 0, received_at = NULL,
                redrive_count = redrive_count + 1, max_deliveries = :maximum, available_at = :available,
                terminal_at = NULL, dedup_until = NULL WHERE row_id = :id", [
                'maximum' => $this->config->maxDeliveries, 'available' => Connection::format($this->connection->after($now, $delay)), 'id' => $ledger->string('row_id'),
            ]);
            $failures = $this->failures();
            $this->connection->execute("UPDATE $failures SET status = 'redriven', redrive_delay_ms = :delay, redrive_generation = :generation,
                resolved_at = :resolved, retained_until = :retained WHERE failure_id = :id", [
                'delay' => $delay, 'generation' => $generation + 1, 'resolved' => Connection::format($now),
                'retained' => $this->retained($now, $this->config->failureRetentionMilliseconds), 'id' => $id,
            ]);
        });
    }

    public function discard(string $id): void
    {
        $this->connection->transaction(function () use ($id): void {
            [$ledger, $failure] = $this->administrativeRows($id);
            if ($failure->string('status') === 'discarded') {
                return;
            }
            if ($failure->string('status') === 'redriven') {
                throw new FailureStateConflictException('Discarding historical failure must not cancel its redriven run.');
            }
            $ledger = $this->pendingLedger($ledger, $failure);
            $now = $this->connection->now();
            $table = $this->ledger();
            $this->connection->execute("UPDATE $table SET state = 'discarded', current_failure_id = NULL, terminal_at = :now, dedup_until = :retained WHERE row_id = :id", [
                'now' => Connection::format($now), 'retained' => $this->retained($now, $this->config->dedupRetentionMilliseconds), 'id' => $ledger->string('row_id'),
            ]);
            $failures = $this->failures();
            $this->connection->execute("UPDATE $failures SET status = 'discarded', resolved_at = :resolved, retained_until = :retained WHERE failure_id = :id", [
                'resolved' => Connection::format($now), 'retained' => $this->retained($now, $this->config->failureRetentionMilliseconds), 'id' => $id,
            ]);
        });
    }

    public function cleanup(int $limit): CleanupResult
    {
        if ($limit < 1) {
            throw new \InvalidArgumentException('Cleanup requires a positive operation budget.');
        }
        return $this->connection->transaction(function () use ($limit): CleanupResult {
            $remaining = $limit;
            $deleted = $removed = $detached = 0;
            $table = $this->ledger();
            $failures = $this->failures();
            $bound = Connection::format($this->connection->now());
            // Acquire every ledger lock before any failure lock. Resolved history can
            // expire while a later generation is active and must not stall cleanup.
            $ledgers = $this->connection->rows("SELECT m.row_id, m.state, m.dedup_until, m.current_failure_id FROM $table AS m
                WHERE m.queue_name = :queue AND ((m.state IN ('completed', 'discarded') AND m.dedup_until <= :terminal_bound)
                    OR EXISTS (SELECT 1 FROM $failures AS f WHERE f.ledger_id = m.row_id AND f.status <> 'pending' AND f.retained_until <= :history_bound))
                ORDER BY m.row_id LIMIT :limit FOR UPDATE OF m SKIP LOCKED", [
                'queue' => $this->config->queue, 'terminal_bound' => $bound, 'history_bound' => $bound, 'limit' => $limit,
            ]);
            foreach ($ledgers as $ledger) {
                if ($remaining === 0) {
                    break;
                }
                $now = $this->connection->now();
                $dedupUntil = $ledger->nullableInstant('dedup_until');
                $terminalDue = in_array($ledger->string('state'), ['completed', 'discarded'], true)
                    && $dedupUntil !== null && $dedupUntil <= $now;
                $filter = $terminalDue ? '' : " AND status <> 'pending' AND retained_until <= :bound";
                $parameters = ['id' => $ledger->string('row_id'), 'limit' => $remaining];
                if (!$terminalDue) {
                    $parameters['bound'] = Connection::format($now);
                }
                $history = $this->connection->rows("SELECT failure_id, status, retained_until FROM $failures WHERE ledger_id = :id $filter ORDER BY failure_id LIMIT :limit FOR UPDATE SKIP LOCKED", $parameters);
                foreach ($history as $item) {
                    if ($item->string('status') === 'pending' || $ledger->nullableString('current_failure_id') === $item->string('failure_id')) {
                        throw new TransportException('Cleanup cannot resolve the current pending failure.');
                    }
                    $retainedUntil = $item->nullableInstant('retained_until');
                    if ($retainedUntil !== null && $retainedUntil <= $now) {
                        $this->connection->execute("DELETE FROM $failures WHERE failure_id = :id", ['id' => $item->string('failure_id')]);
                        ++$removed;
                    } else {
                        $this->connection->execute("UPDATE $failures SET ledger_id = NULL WHERE failure_id = :id", ['id' => $item->string('failure_id')]);
                        ++$detached;
                    }
                    --$remaining;
                }
                if ($terminalDue && $remaining > 0 && $this->connection->row("SELECT 1 FROM $failures WHERE ledger_id = :id LIMIT 1", ['id' => $ledger->string('row_id')]) === null) {
                    $this->connection->execute("DELETE FROM $table WHERE row_id = :id", ['id' => $ledger->string('row_id')]);
                    ++$deleted;
                    --$remaining;
                }
            }
            if ($remaining > 0) {
                $history = $this->connection->rows("SELECT failure_id, retained_until FROM $failures
                    WHERE queue_name = :queue AND ledger_id IS NULL AND status <> 'pending' AND retained_until <= :bound
                    ORDER BY failure_id LIMIT :limit FOR UPDATE SKIP LOCKED", [
                    'queue' => $this->config->queue, 'bound' => Connection::format($this->connection->now()), 'limit' => $remaining,
                ]);
                foreach ($history as $failure) {
                    if ($failure->instant('retained_until') > $this->connection->now()) {
                        continue;
                    }
                    $this->connection->execute("DELETE FROM $failures WHERE failure_id = :id", ['id' => $failure->string('failure_id')]);
                    ++$removed;
                }
            }
            return new CleanupResult($deleted, $removed, $detached);
        });
    }

    public function metadata(Row $row): DeliveryMetadata
    {
        return new DeliveryMetadata($this->config->queue, $row->integer('retry_count'), $row->integer('delivery_count'), CountAccuracy::Exact, $row->instant('received_at'));
    }

    public static function raw(Row $row): RawMessage
    {
        $headers = json_decode($row->string('raw_headers'), true, 512, JSON_THROW_ON_ERROR);
        if (!is_array($headers)) {
            throw new \UnexpectedValueException('Stored raw headers must decode to a map.');
        }
        return new RawMessage($row->string('raw_body'), new Headers($headers));
    }

    /** @return array{Row,\DateTimeImmutable} */
    private function owned(string $id, string $token): array
    {
        $table = $this->ledger();
        $row = $this->connection->row("SELECT * FROM $table WHERE queue_name = :queue AND row_id = :id FOR UPDATE", ['queue' => $this->config->queue, 'id' => $id]);
        $now = $this->connection->now();
        if ($row === null || $row->string('state') !== 'leased' || $row->nullableString('lease_token') !== $token || $row->instant('lease_until') <= $now) {
            throw new DeliveryLostException('This reservation no longer owns the queue record.');
        }
        return [$row, $now];
    }

    private function recordFailure(Row $row, FailureDetails $details, \DateTimeImmutable $now): void
    {
        $id = self::id();
        $failures = $this->failures();
        $this->connection->execute("INSERT INTO $failures (failure_id, ledger_id, queue_name, logical_id, generation, raw_body, raw_headers, retry_count, delivery_count, received_at, failed_at, reason, exception_type, description, status)
            VALUES (:id, :ledger, :queue, :logical_id, :generation, :body, :headers, :retry_count, :delivery_count, :received_at, :failed_at, :reason, :exception_type, :description, 'pending')", [
            'id' => $id, 'ledger' => $row->string('row_id'), 'queue' => $this->config->queue, 'logical_id' => $row->string('logical_id'), 'generation' => $row->integer('generation'),
            'body' => new Parameter($row->string('raw_body'), \PDO::PARAM_LOB), 'headers' => new Parameter($row->string('raw_headers'), \PDO::PARAM_LOB),
            'retry_count' => $row->integer('retry_count'), 'delivery_count' => $row->integer('delivery_count'), 'received_at' => $row->string('received_at'),
            'failed_at' => Connection::format($now), 'reason' => $details->reason->value, 'exception_type' => $details->exceptionType, 'description' => $details->description,
        ]);
        $table = $this->ledger();
        $this->connection->execute("UPDATE $table SET state = 'failed', current_failure_id = :failure, lease_token = NULL, lease_until = NULL WHERE row_id = :id", ['failure' => $id, 'id' => $row->string('row_id')]);
    }

    /** @return array{?Row,Row} */
    private function administrativeRows(string $id): array
    {
        $failures = $this->failures();
        $lookup = $this->connection->row("SELECT ledger_id FROM $failures WHERE queue_name = :queue AND failure_id = :id", ['queue' => $this->config->queue, 'id' => $id]);
        $ledger = null;
        if ($lookup !== null && $lookup->nullableString('ledger_id') !== null) {
            $table = $this->ledger();
            $ledger = $this->connection->row("SELECT * FROM $table WHERE queue_name = :queue AND row_id = :id FOR UPDATE", ['queue' => $this->config->queue, 'id' => $lookup->nullableString('ledger_id')]);
        }
        $failure = $this->connection->row("SELECT * FROM $failures WHERE queue_name = :queue AND failure_id = :id FOR UPDATE", ['queue' => $this->config->queue, 'id' => $id]);
        if ($failure === null) {
            throw new FailureNotFoundException('The failure record does not exist in this queue.');
        }
        if ($failure->string('status') === 'pending' && ($ledger === null || $failure->nullableString('ledger_id') !== $ledger->string('row_id'))) {
            throw new TransportException('A pending failure lost its ledger incarnation.');
        }
        return [$ledger, $failure];
    }

    private function pendingLedger(?Row $ledger, Row $failure): Row
    {
        if ($ledger === null || $ledger->string('state') !== 'failed' || $ledger->nullableString('current_failure_id') !== $failure->string('failure_id') || $ledger->integer('generation') !== $failure->integer('generation')) {
            throw new TransportException('The failure record does not own the current failed processing generation.');
        }
        return $ledger;
    }

    private function summary(Row $row): FailureSummary
    {
        return new FailureSummary($row->string('failure_id'), $this->config->queue, $row->string('logical_id'), $row->instant('failed_at'), FailureReason::from($row->string('reason')), FailureStatus::from($row->string('status')));
    }

    private function decodeCursor(FailureQuery $query): int
    {
        if ($query->cursor === null) {
            return 0;
        }
        try {
            if (preg_match('/\A[A-Za-z0-9_-]+\z/', $query->cursor) !== 1 || strlen($query->cursor) > 1024) {
                throw new \InvalidArgumentException('Invalid failure cursor.');
            }
            $json = base64_decode(strtr($query->cursor, '-_', '+/'), true);
            $data = $json === false ? null : json_decode($json, true, 8, JSON_THROW_ON_ERROR);
            if (!is_array($data) || array_keys($data) !== ['scope', 'status', 'after'] || $data['scope'] !== $this->connection->scope($this->config->queue)
                || $data['status'] !== $query->status?->value || !is_int($data['after']) || $data['after'] < 1) {
                throw new \InvalidArgumentException('The cursor is invalid or belongs to a different queue/filter.');
            }
            return $data['after'];
        } catch (\JsonException $error) {
            throw new \InvalidArgumentException('Invalid failure cursor JSON.', 0, $error);
        }
    }

    private function encodeCursor(int $after, ?FailureStatus $status): string
    {
        return rtrim(strtr(base64_encode(json_encode(['scope' => $this->connection->scope($this->config->queue), 'status' => $status?->value, 'after' => $after], JSON_THROW_ON_ERROR)), '+/', '-_'), '=');
    }

    private function retained(\DateTimeImmutable $now, ?int $milliseconds): ?string
    {
        return $milliseconds === null ? null : Connection::format($this->connection->after($now, $milliseconds));
    }

    private function ledger(): string
    {
        return $this->connection->table('queue_messages');
    }

    private function failures(): string
    {
        return $this->connection->table('queue_failures');
    }

    private static function headers(RawMessage $raw): string
    {
        $values = $raw->headers->values;
        ksort($values, SORT_STRING);
        return json_encode((object) $values, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
    }

    private static function id(): string
    {
        return bin2hex(random_bytes(16));
    }
}

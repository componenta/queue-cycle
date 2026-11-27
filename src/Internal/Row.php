<?php

declare(strict_types=1);

namespace Componenta\Queue\Cycle\Internal;

use DateTimeImmutable;
use UnexpectedValueException;

/** @internal Validates driver results before typed queue values are constructed. */
final readonly class Row
{
    /** @var array<string,mixed> */
    private array $values;

    public function __construct(mixed $values)
    {
        if (!is_array($values)) {
            throw new UnexpectedValueException('The database did not return an associative row.');
        }

        $materialized = [];
        foreach ($values as $name => $value) {
            if (!is_string($name)) {
                throw new UnexpectedValueException('The database row contains a non-string column name.');
            }
            if (is_resource($value)) {
                $stream = $value;
                try {
                    $bytes = stream_get_contents($stream);
                    if ($bytes === false) {
                        throw new UnexpectedValueException('A database binary field could not be read.');
                    }
                    $value = $bytes;
                } finally {
                    fclose($stream);
                }
            }
            $materialized[$name] = $value;
        }
        $this->values = $materialized;
    }

    public function string(string $column): string
    {
        $value = $this->value($column);
        if (!is_string($value)) {
            throw new UnexpectedValueException('A database text or binary field has an invalid type: ' . $column);
        }
        return $value;
    }

    public function nullableString(string $column): ?string
    {
        $value = $this->value($column);
        if ($value !== null && !is_string($value)) {
            throw new UnexpectedValueException('A nullable database field has an invalid type: ' . $column);
        }
        return $value;
    }

    public function integer(string $column): int
    {
        $value = $this->value($column);
        if (is_int($value)) {
            return $value;
        }
        if (is_string($value) && preg_match('/\A-?(?:0|[1-9][0-9]*)\z/', $value) === 1) {
            $integer = (int) $value;
            if ((string) $integer === $value) {
                return $integer;
            }
        }
        throw new UnexpectedValueException('A database integer cannot be represented exactly: ' . $column);
    }

    public function instant(string $column): DateTimeImmutable
    {
        return new DateTimeImmutable($this->string($column));
    }

    public function nullableInstant(string $column): ?DateTimeImmutable
    {
        $value = $this->nullableString($column);
        return $value === null ? null : new DateTimeImmutable($value);
    }

    private function value(string $column): mixed
    {
        if (!array_key_exists($column, $this->values)) {
            throw new UnexpectedValueException('The database row is missing a required field: ' . $column);
        }
        return $this->values[$column];
    }
}

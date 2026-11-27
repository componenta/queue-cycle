<?php

declare(strict_types=1);

namespace Componenta\Queue\Cycle\Tests\Support;

use Componenta\Queue\Cycle\Tests\Fixture;

/** A separate PHP process and physical connection, synchronized through line messages. */
final class Process
{
    /** @var resource */
    private $process;
    /** @var array<int,resource> */
    private array $pipes = [];
    public readonly int $backendPid;

    /** @param array<string,mixed> $overrides */
    public function __construct(Fixture $fixture, array $overrides = [])
    {
        $this->process = proc_open([
            PHP_BINARY, __DIR__ . '/worker.php',
            base64_encode(json_encode(['schema' => $fixture->schema, 'overrides' => $overrides], JSON_THROW_ON_ERROR)),
        ], [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $this->pipes);
        if (!is_resource($this->process)) {
            throw new \RuntimeException('Could not start an independent PHP worker.');
        }
        $this->backendPid = $this->read()['pid'];
    }

    /** @param array<string,mixed> $command */
    public function send(array $command): void
    {
        $line = json_encode($command, JSON_THROW_ON_ERROR) . "\n";
        if (fwrite($this->pipes[0], $line) !== strlen($line)) {
            throw new \RuntimeException('Could not send a complete process command.');
        }
        fflush($this->pipes[0]);
    }

    /** @return array<string,mixed> */
    public function read(int $timeoutSeconds = 5): array
    {
        stream_set_timeout($this->pipes[1], $timeoutSeconds);
        $line = fgets($this->pipes[1]);
        if ($line === false) {
            stream_set_blocking($this->pipes[2], false);
            throw new \RuntimeException('Worker response missing: ' . stream_get_contents($this->pipes[2]));
        }
        return json_decode($line, true, 32, JSON_THROW_ON_ERROR);
    }

    /** @param array<string,mixed> $command @return array<string,mixed> */
    public function request(array $command): array
    {
        $this->send($command);
        return $this->read();
    }

    public function awaitLock(Fixture $fixture): void
    {
        $deadline = hrtime(true) + 2_000_000_000;
        do {
            $row = $fixture->row("SELECT wait_event_type FROM pg_stat_activity WHERE pid = :pid", ['pid' => $this->backendPid]);
            if ($row['wait_event_type'] === 'Lock') {
                return;
            }
            if (hrtime(true) > $deadline) {
                throw new \RuntimeException('The independent worker did not reach its lock barrier.');
            }
            usleep(1_000);
        } while (true);
    }

    public function close(): void
    {
        if (!is_resource($this->process)) {
            return;
        }
        $status = proc_get_status($this->process);
        if ($status['running']) {
            proc_terminate($this->process);
        }
        foreach ($this->pipes as $pipe) {
            fclose($pipe);
        }
        proc_close($this->process);
    }

    public function crash(): void
    {
        if (!is_resource($this->process) || !proc_terminate($this->process, 9)) {
            throw new \RuntimeException('Could not kill the owned test worker.');
        }
        foreach ($this->pipes as $pipe) {
            fclose($pipe);
        }
        proc_close($this->process);
    }

    public function __destruct()
    {
        $this->close();
    }
}

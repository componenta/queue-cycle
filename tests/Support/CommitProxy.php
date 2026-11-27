<?php

declare(strict_types=1);

namespace Componenta\Queue\Cycle\Tests\Support;

/** A real TCP PostgreSQL relay which can drop one COMMIT or only its response. */
final class CommitProxy
{
    /** @var resource */
    private $process;
    /** @var array<int,resource> */
    private array $pipes = [];
    public readonly string $dsn;

    public function __construct(string $mode, int $commitNumber = 1)
    {
        $dsn = (string) getenv('QUEUE_TEST_DSN');
        if (preg_match('/(?:^pgsql:|;)host=([^;]+)/', $dsn, $host) !== 1) {
            throw new \RuntimeException('The commit proxy requires an explicit TCP host in QUEUE_TEST_DSN.');
        }
        preg_match('/;port=([0-9]+)/', $dsn, $port);
        $this->process = proc_open(
            [PHP_BINARY, __DIR__ . '/commit-proxy.php', $host[1], $port[1] ?? '5432', $mode, (string) $commitNumber],
            [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $this->pipes
        );
        if (!is_resource($this->process)) {
            throw new \RuntimeException('Could not start the PostgreSQL commit proxy.');
        }
        $ready = $this->read();
        $dsn = preg_replace('/(?:^pgsql:|;)host=[^;]+/', 'pgsql:host=127.0.0.1', $dsn);
        $dsn = preg_replace('/;(?:port|sslmode|gssencmode)=[^;]+/', '', $dsn);
        $this->dsn = $dsn . ';port=' . $ready['port'] . ';sslmode=disable;gssencmode=disable';
    }

    /** @return array<string,mixed> */
    public function read(): array
    {
        stream_set_timeout($this->pipes[1], 5);
        $line = fgets($this->pipes[1]);
        if ($line === false) {
            stream_set_blocking($this->pipes[2], false);
            throw new \RuntimeException('Proxy response missing: ' . stream_get_contents($this->pipes[2]));
        }
        return json_decode($line, true, 8, JSON_THROW_ON_ERROR);
    }

    public function close(): void
    {
        if (!is_resource($this->process)) {
            return;
        }
        if (proc_get_status($this->process)['running']) {
            proc_terminate($this->process);
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

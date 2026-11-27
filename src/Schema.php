<?php

declare(strict_types=1);

namespace Componenta\Queue\Cycle;

use Componenta\Queue\Cycle\Internal\Connection;
use Cycle\Database\DatabaseInterface;

final class Schema
{
    public static function install(DatabaseInterface $database): void
    {
        $connection = new Connection($database, 5_000);
        $connection->transaction(static function () use ($connection): void {
            $sql = file_get_contents(dirname(__DIR__) . '/resources/schema.sql');
            if ($sql === false) {
                throw new \RuntimeException('The packaged queue schema could not be read.');
            }
            // Split the bundled template before identifiers can introduce quoted semicolons.
            foreach (explode(';', $sql) as $statement) {
                if (trim($statement) !== '') {
                    $statement = strtr($statement, ['{{ledger}}' => $connection->table('queue_messages'), '{{failures}}' => $connection->table('queue_failures')]);
                    $connection->execute($statement);
                }
            }
            // Persist a non-secret installation identity without another table.
            // A recreated failure table receives a new namespace for its cursors.
            $marker = Connection::INSTALLATION_MARKER_PREFIX . bin2hex(random_bytes(16));
            $connection->execute('COMMENT ON TABLE ' . $connection->table('queue_failures') . " IS '$marker'");
        });
    }
}

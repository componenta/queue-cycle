<?php

declare(strict_types=1);

use Componenta\Queue\Cycle\Schema;
use Componenta\Queue\Exception\TransportException;
use Componenta\Queue\FailureDetails;
use Componenta\Queue\FailureQuery;
use Componenta\Queue\FailureReason;
use Cycle\Database\DatabaseInterface;

it('installs and isolates a queue in a schema whose quoted name contains punctuation', function (string $suffix): void {
    $schema = $this->fixture->schema . $suffix;
    $quoted = '"' . str_replace('"', '""', $schema) . '"';
    $this->fixture->execute('CREATE SCHEMA ' . $quoted);
    $database = $this->fixture->newDatabase();
    $writer = $database->getDriver(DatabaseInterface::WRITE);

    try {
        $writer->execute('SET search_path TO ' . $quoted);
        Schema::install($database);
        $queue = $this->fixture->queue(database: $database);
        $message = $this->fixture->message();
        $queue->publisher()->publish($message);
        $receiver = $queue->receiver();
        $delivery = $receiver->receive();

        expect($this->fixture->codec->decode($delivery->raw()))->toEqual($message);
        $delivery->fail(new FailureDetails(FailureReason::Handler));
        $page = $queue->failures()->page(new FailureQuery(1));
        expect($page->items)->toHaveCount(1)
            ->and($page->items[0]->messageId)->toBe($message->id)
            ->and($this->fixture->queue()->failures()->page(new FailureQuery(1))->items)->toBeEmpty();
        $queue->failures()->redrive($page->items[0]->failureId);
        $receiver->receive()->acknowledge();
        expect($receiver->receive())->toBeNull()
            ->and($this->fixture->row('SELECT count(*) AS count FROM ' . $this->fixture->table())['count'])->toBe(0);
        $receiver->close();
    } finally {
        $this->fixture->execute('DROP SCHEMA ' . $quoted . ' CASCADE');
    }
})->with([
    'statement delimiter in an identifier' => ';tenant',
    'statement delimiter with a quoted identifier character' => ';tenant"blue',
    'quoted-name control without a statement delimiter' => ' tenant"blue',
    'another template token inside an identifier' => '{{failures}}',
    'same template token inside an identifier' => '{{ledger}}',
]);

it('rolls back a partial installation when an existing table rejects a later statement', function (): void {
    $ledger = $this->fixture->table();
    $failures = $this->fixture->table('queue_failures');
    $this->fixture->execute('DROP TABLE ' . $failures);
    $this->fixture->execute('DROP TABLE ' . $ledger);
    $this->fixture->execute('CREATE TABLE ' . $failures . ' (sentinel text NOT NULL)');
    $this->fixture->execute('INSERT INTO ' . $failures . ' VALUES (:value)', ['value' => 'pre-existing']);

    expect(fn () => Schema::install($this->fixture->database))->toThrow(TransportException::class);

    expect($this->fixture->row('SELECT to_regclass(:table) AS relation', ['table' => $ledger])['relation'])->toBeNull()
        ->and($this->fixture->row('SELECT sentinel FROM ' . $failures)['sentinel'])->toBe('pre-existing')
        ->and($this->fixture->database->getDriver(DatabaseInterface::WRITE)->getTransactionLevel())->toBe(0);
});

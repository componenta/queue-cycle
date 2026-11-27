<?php

declare(strict_types=1);

$loader = require dirname(__DIR__) . '/vendor/autoload.php';
$corePath = \Composer\InstalledVersions::getInstallPath('componenta/queue');
if ($corePath === null || !is_file($corePath . '/tests/Support/PortableQueueContract.php')) {
    throw new RuntimeException('The installed core must include its portable adapter conformance suite.');
}
$loader->addPsr4('Componenta\\Queue\\Tests\\Support\\', $corePath . '/tests/Support');

if (getenv('QUEUE_TEST_DSN') === false) {
    throw new RuntimeException('Set QUEUE_TEST_DSN to an isolated PostgreSQL test database; no database is selected implicitly.');
}

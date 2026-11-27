<?php

declare(strict_types=1);

// Generate an explicit development root without changing production constraints.
$root = dirname(__DIR__);
$core = realpath($argv[1] ?? $root . '/../componenta-queue');
if ($core === false || !is_file($core . '/composer.json')) {
    throw new RuntimeException('Pass the path to a componenta/queue development checkout.');
}
$coreManifest = json_decode(file_get_contents($core . '/composer.json'), true, 512, JSON_THROW_ON_ERROR);
if (($coreManifest['name'] ?? null) !== 'componenta/queue') {
    throw new RuntimeException('The checkout must contain componenta/queue.');
}
$manifest = json_decode(file_get_contents($root . '/composer.json'), true, 512, JSON_THROW_ON_ERROR);
$manifest['require']['componenta/queue'] = '^1.0@dev';
$manifest['repositories'] = [[
    'type' => 'path',
    'url' => $core,
    'options' => ['symlink' => true, 'versions' => ['componenta/queue' => '1.0.x-dev']],
]];
file_put_contents($root . '/composer.dev.json', json_encode($manifest, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR) . "\n");
fwrite(STDOUT, "Generated composer.dev.json for the 1.0 development line.\n");

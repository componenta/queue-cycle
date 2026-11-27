<?php

declare(strict_types=1);

// Test-only PostgreSQL protocol relay. It never fabricates a driver exception.
$server = stream_socket_server('tcp://127.0.0.1:0', $errorNumber, $errorText);
if ($server === false) {
    throw new RuntimeException($errorText, $errorNumber);
}
$address = stream_socket_get_name($server, false);
fwrite(STDOUT, json_encode(['port' => (int) substr($address, strrpos($address, ':') + 1)], JSON_THROW_ON_ERROR) . "\n");
fflush(STDOUT);
$front = stream_socket_accept($server, 5);
if ($front === false) {
    throw new RuntimeException('No client connected to the test proxy.');
}
$back = stream_socket_client('tcp://' . $argv[1] . ':' . $argv[2], $errorNumber, $errorText, 5);
if ($back === false) {
    throw new RuntimeException($errorText, $errorNumber);
}
stream_set_timeout($front, 5);
stream_set_timeout($back, 5);
$read = static function ($stream, int $length): string {
    $data = '';
    while (strlen($data) < $length) {
        $part = fread($stream, $length - strlen($data));
        if ($part === false || $part === '') {
            throw new RuntimeException('Proxy peer closed before a complete frame.');
        }
        $data .= $part;
    }
    return $data;
};
$write = static function ($stream, string $data): void {
    while ($data !== '') {
        $length = fwrite($stream, $data);
        if ($length === false || $length === 0) {
            throw new RuntimeException('Proxy could not forward a frame.');
        }
        $data = substr($data, $length);
    }
};
do {
    $length = $read($front, 4);
    $body = $read($front, unpack('Nlength', $length)['length'] - 4);
    $protocol = unpack('Nversion', substr($body, 0, 4))['version'];
    if ($protocol === 80877103 || $protocol === 80877104) {
        $write($front, 'N');
    } else {
        $write($back, $length . $body);
    }
} while ($protocol === 80877103 || $protocol === 80877104);

$commits = 0;
$suppress = false;
$deadline = hrtime(true) + 15_000_000_000;
while (hrtime(true) < $deadline) {
    $streams = [$front, $back];
    $writable = $exception = [];
    if (stream_select($streams, $writable, $exception, 1) === false) {
        throw new RuntimeException('Proxy select failed.');
    }
    foreach ($streams as $source) {
        if (feof($source)) {
            exit(0);
        }
        $type = $read($source, 1);
        $length = $read($source, 4);
        $bodyLength = unpack('Nlength', $length)['length'] - 4;
        $body = $bodyLength === 0 ? '' : $read($source, $bodyLength);
        if ($source === $front) {
            if ($type === 'Q' && strtoupper(trim($body, " \t\n\r\0;")) === 'COMMIT' && ++$commits === (int) $argv[4]) {
                if ($argv[3] === 'before') {
                    fclose($front);
                    fclose($back);
                    fwrite(STDOUT, "{\"boundary\":\"before\"}\n");
                    exit(0);
                }
                $suppress = $argv[3] === 'after';
            }
            $write($back, $type . $length . $body);
        } elseif (!$suppress) {
            $write($front, $type . $length . $body);
        } elseif ($type === 'Z') {
            // ReadyForQuery after the forwarded COMMIT proves the server completed it.
            fclose($front);
            fclose($back);
            fwrite(STDOUT, "{\"boundary\":\"after\"}\n");
            exit(0);
        }
    }
}
throw new RuntimeException('The test proxy exceeded its bounded lifetime.');

<?php
declare(strict_types=1);

/**
 * Planfix filtered task downloader (v2)
 * - Uses saved filter ID (default 268518)
 * - Paginates until all tasks are fetched
 * - Writes JSON output to planfix_downloads/
 *
 * Usage (CLI):
 *   php planfix-filtered-download-v2.php [filterId] [pageSize]
 *     - filterId: numeric saved filter id (default: 268518)
 *     - pageSize: items per page (default: 200)
 *
 * Usage (Web):
 *   planfix-filtered-download-v2.php?filter=268518&pageSize=200
 */

// -----------------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------------
$planfixConfig = [
    'accountName' => 'chemwatch',
    'apiToken' => 'd8123b13d3951f2dcfed8d76626a4dea',
    'baseUrl' => 'https://chemwatch.planfix.com/rest',
    'fields' => 'id,name,description,status,workers,project,startDateTime,endDateTime,template,customData,owner,assigner,assignees,participants,auditors,priority,tags,checkList'
];

$defaultFilterId = 268518;
$defaultPageSize = 200; // safe default; script will loop until complete
$downloadDir = __DIR__ . '/planfix_downloads';

// -----------------------------------------------------------------------------
// Parameter resolution
// -----------------------------------------------------------------------------
$isCli = php_sapi_name() === 'cli';
$filterId = $defaultFilterId;
$pageSize = $defaultPageSize;

if ($isCli) {
    if (isset($argv[1]) && is_numeric($argv[1])) {
        $filterId = (int)$argv[1];
    }
    if (isset($argv[2]) && is_numeric($argv[2])) {
        $pageSize = max(1, (int)$argv[2]);
    }
} else {
    if (isset($_GET['filter']) && is_numeric($_GET['filter'])) {
        $filterId = (int)$_GET['filter'];
    } elseif (isset($_GET['filterId']) && is_numeric($_GET['filterId'])) {
        $filterId = (int)$_GET['filterId'];
    } elseif (isset($_GET['filter_id']) && is_numeric($_GET['filter_id'])) {
        $filterId = (int)$_GET['filter_id'];
    }
    if (isset($_GET['pageSize']) && is_numeric($_GET['pageSize'])) {
        $pageSize = max(1, (int)$_GET['pageSize']);
    }
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------
/**
 * Ensure output directory exists
 */
function ensureDirectory(string $dir): void {
    if (!is_dir($dir)) {
        if (!mkdir($dir, 0755, true) && !is_dir($dir)) {
            throw new RuntimeException('Unable to create directory: ' . $dir);
        }
    }
}

/**
 * Perform a JSON POST request to Planfix
 */
function planfixRequest(array $config, string $endpoint, array $body): array {
    $url = rtrim($config['baseUrl'], '/') . $endpoint;

    $ch = curl_init($url);
    $payload = json_encode($body);

    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_POST => true,
        CURLOPT_POSTFIELDS => $payload,
        CURLOPT_HTTPHEADER => [
            'Authorization: Bearer ' . $config['apiToken'],
            'Content-Type: application/json',
            'Accept: application/json',
        ],
        CURLOPT_TIMEOUT => 30,
        CURLOPT_CONNECTTIMEOUT => 10,
        CURLOPT_FOLLOWLOCATION => true,
    ]);

    $responseBody = curl_exec($ch);
    $httpCode = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $curlErr = curl_error($ch);
    curl_close($ch);

    if ($responseBody === false) {
        return [
            'success' => false,
            'httpCode' => $httpCode,
            'error' => 'cURL error: ' . $curlErr,
            'data' => null,
        ];
    }

    $decoded = json_decode($responseBody, true);
    if (json_last_error() !== JSON_ERROR_NONE) {
        return [
            'success' => false,
            'httpCode' => $httpCode,
            'error' => 'JSON decode error: ' . json_last_error_msg(),
            'raw' => substr($responseBody, 0, 500),
            'data' => null,
        ];
    }

    if ($httpCode !== 200) {
        return [
            'success' => false,
            'httpCode' => $httpCode,
            'error' => 'HTTP ' . $httpCode,
            'data' => $decoded,
        ];
    }

    return [
        'success' => true,
        'httpCode' => $httpCode,
        'data' => $decoded,
    ];
}

/**
 * Build the request body for listing tasks with pagination and a saved filter
 */
function buildListBody(array $config, int $filterId, int $offset, int $pageSize): array {
    return [
        'fields' => $config['fields'],
        'pageSize' => $pageSize,
        'offset' => $offset,
        // Saved filter: most Planfix endpoints accept numeric 'filter' for saved filters
        'filter' => $filterId,
    ];
}

/**
 * Try a set of candidate endpoints and return the first that responds with HTTP 200
 */
function detectWorkingEndpoint(array $config, int $filterId, array $probeBody): ?string {
    $candidates = [
        '/task/list',              // common list endpoint supporting saved filter
        '/filter/' . $filterId . '/task',
        '/filter/' . $filterId . '/tasks',
    ];

    foreach ($candidates as $endpoint) {
        $probe = planfixRequest($config, $endpoint, $probeBody);
        if ($probe['success'] === true) {
            return $endpoint;
        }
    }

    return null;
}

/**
 * Extract tasks array and total count from a Planfix response
 */
function parseTasksResponse(array $response): array {
    // Tasks may be under different keys depending on endpoint
    $tasks = [];
    if (isset($response['tasks']) && is_array($response['tasks'])) {
        $tasks = $response['tasks'];
    } elseif (isset($response['data']) && is_array($response['data'])) {
        $tasks = $response['data'];
    } elseif (isset($response[0]) || empty($response)) {
        // Sometimes the response itself is an array of tasks
        $tasks = is_array($response) ? $response : [];
    }

    $total = $response['total']
        ?? $response['totalCount']
        ?? $response['count']
        ?? null;

    return [
        'tasks' => $tasks,
        'total' => is_numeric($total) ? (int)$total : null,
    ];
}

// -----------------------------------------------------------------------------
// Execution
// -----------------------------------------------------------------------------
try {
    ensureDirectory($downloadDir);

    $offset = 0;
    $collectedTasks = [];

    // Probe with pageSize=1 to discover a working endpoint
    $probeBody = buildListBody($planfixConfig, $filterId, 0, 1);
    $endpoint = detectWorkingEndpoint($planfixConfig, $filterId, $probeBody);

    if ($endpoint === null) {
        $message = 'No working API endpoints found for saved filter ID ' . $filterId;
        if (!$isCli) {
            header('Content-Type: application/json');
            http_response_code(502);
            echo json_encode(['success' => false, 'error' => $message]);
        } else {
            fwrite(STDERR, $message . PHP_EOL);
        }
        exit(1);
    }

    // Iterate pages until no more tasks or until total reached
    $expectedTotal = null; // The API might return a total count in the first response

    while (true) {
        $body = buildListBody($planfixConfig, $filterId, $offset, $pageSize);
        $result = planfixRequest($planfixConfig, $endpoint, $body);

        if ($result['success'] !== true) {
            $err = 'Request failed at offset ' . $offset . ': ' . ($result['error'] ?? 'unknown error');
            if (!$isCli) {
                header('Content-Type: application/json');
                http_response_code($result['httpCode'] ?? 500);
                echo json_encode(['success' => false, 'error' => $err]);
            } else {
                fwrite(STDERR, $err . PHP_EOL);
            }
            exit(2);
        }

        $parsed = parseTasksResponse($result['data']);
        $pageTasks = $parsed['tasks'];
        if ($expectedTotal === null && $parsed['total'] !== null) {
            $expectedTotal = $parsed['total'];
        }

        if (!is_array($pageTasks) || count($pageTasks) === 0) {
            break;
        }

        // Merge tasks
        foreach ($pageTasks as $task) {
            $collectedTasks[] = $task;
        }

        // Advance pagination
        $offset += count($pageTasks);

        // Stop if we've reached the known total
        if ($expectedTotal !== null && $offset >= $expectedTotal) {
            break;
        }

        // If fewer than requested came back, we've reached the end
        if (count($pageTasks) < $pageSize) {
            break;
        }
    }

    // Output file
    $timestamp = date('Y-m-d_H-i-s');
    $filename = sprintf(
        '%s/chemwatch_tasks_filter_%d_%s.json',
        $downloadDir,
        $filterId,
        $timestamp
    );

    $output = [
        'account' => $planfixConfig['accountName'],
        'filter_id' => $filterId,
        'total_downloaded' => count($collectedTasks),
        'expected_total' => $expectedTotal,
        'downloaded_at' => date('c'),
        'tasks' => $collectedTasks,
    ];

    file_put_contents($filename, json_encode($output, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE));

    if (!$isCli) {
        header('Content-Type: application/json');
        echo json_encode([
            'success' => true,
            'filter_id' => $filterId,
            'endpoint' => $endpoint,
            'page_size' => $pageSize,
            'total_downloaded' => count($collectedTasks),
            'expected_total' => $expectedTotal,
            'file' => basename($filename),
        ]);
    } else {
        $totalInfo = $expectedTotal !== null ? (' (expected ' . $expectedTotal . ')') : '';
        echo 'Downloaded ' . count($collectedTasks) . ' tasks' . $totalInfo . ' using filter ' . $filterId . PHP_EOL;
        echo 'Saved to: ' . $filename . PHP_EOL;
    }

    exit(0);
} catch (Throwable $e) {
    if (!$isCli) {
        header('Content-Type: application/json');
        http_response_code(500);
        echo json_encode(['success' => false, 'error' => $e->getMessage()]);
    } else {
        fwrite(STDERR, 'Error: ' . $e->getMessage() . PHP_EOL);
    }
    exit(1);
}

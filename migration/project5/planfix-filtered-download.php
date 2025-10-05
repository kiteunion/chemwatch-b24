<?php
/**
 * High-Speed Optimized Planfix Task Downloader with Parallel Processing
 * Features: Multi-threading, connection pooling, concurrent requests, filtering
 * Usage: php planfix_task_downloader.php [batch_number] [threads] [filter_preset]
 * Example: php planfix_task_downloader.php 1 8 recent
 */

// Configuration
$downloadFolder = 'planfix_task_downloads';
$batchSize = 1000; // Tasks per batch
$totalBatches = 10; // Adjust based on your total task count
$defaultThreads = 8; // Concurrent request threads

$planfixConfig = [
    'accountName' => 'chemwatch',
    'apiToken' => 'd8123b13d3951f2dcfed8d76626a4dea',
    'baseUrl' => 'https://chemwatch.planfix.com/rest',
    'fields' => 'id,name,description,status,workers,project,startDateTime,endDateTime,template,customData,owner,assigner,assignees,participants,auditors,priority,tags,checkList'
];

// Filter presets for common use cases
$filterPresets = [
    'all' => [],
    'recent' => [
        'startDateTime' => ['from' => date('Y-m-d', strtotime('-30 days'))]
    ],
    'active' => [
        'status' => [1, 2, 3] // Replace with your active status IDs
    ],
    'this_year' => [
        'startDateTime' => ['from' => date('Y-01-01'), 'to' => date('Y-12-31')]
    ],
    'urgent' => [
        'priority' => [1, 2] // Replace with your urgent priority IDs
    ],
    'filter_268518' => [
        'filter_id' => 268518 // Your specific Planfix filter
    ]
];

class OptimizedPlanfixTaskDownloader {
    private $config;
    private $downloadFolder;
    private $batchSize;
    private $currentBatch;
    private $maxThreads;
    private $logFile;
    private $batchLogFile;
    private $curlMultiHandle;
    private $activeRequests = [];
    private $requestQueue = [];
    private $filters = [];
    private $stats = [
        'batch_number' => 0,
        'batch_start' => '',
        'batch_end' => '',
        'tasks_in_batch' => 0,
        'processed_tasks' => 0,
        'successful_requests' => 0,
        'failed_requests' => 0,
        'concurrent_requests' => 0,
        'peak_concurrent' => 0,
        'total_request_time' => 0,
        'total_tasks_downloaded' => 0,
        'errors' => []
    ];

    // Optimized configuration constants
    const DEFAULT_TIMEOUT = 15;
    const CONNECT_TIMEOUT = 10;
    const MAX_RETRIES = 2;
    const RATE_LIMIT_DELAY = 0.5;
    const CURL_BUFFER_SIZE = 32768;
    const MAX_REDIRECTS = 3;
    const DNS_CACHE_TIMEOUT = 300;

    public function __construct($config, $downloadFolder, $batchSize, $batchNumber, $maxThreads = 8, $filters = []) {
        $this->config = $this->validateConfig($config);
        $this->downloadFolder = rtrim($downloadFolder, '/');
        $this->batchSize = $batchSize;
        $this->currentBatch = $batchNumber;
        $this->maxThreads = $maxThreads;
        $this->filters = $filters;
        $this->logFile = $this->downloadFolder . '/task_download.log';
        $this->batchLogFile = $this->downloadFolder . "/batch_{$batchNumber}.log";

        $this->initializeDirectories();
        $this->initializeCurlMulti();

        $this->stats['batch_number'] = $batchNumber;
        $this->stats['batch_start'] = date('Y-m-d H:i:s');

        $this->log("Optimized Task Batch $batchNumber downloader initialized with $maxThreads threads");
        $this->log("Account: {$this->config['accountName']}, API: {$this->config['baseUrl']}");

        if (!empty($filters)) {
            $this->log("Active filters: " . json_encode($filters));
        }
    }

    /**
     * Initialize cURL multi handle for concurrent requests
     */
    private function initializeCurlMulti() {
        $this->curlMultiHandle = curl_multi_init();

        if (defined('CURLMOPT_MAX_TOTAL_CONNECTIONS')) {
            curl_multi_setopt($this->curlMultiHandle, CURLMOPT_MAX_TOTAL_CONNECTIONS, $this->maxThreads * 2);
        }
        if (defined('CURLMOPT_MAX_HOST_CONNECTIONS')) {
            curl_multi_setopt($this->curlMultiHandle, CURLMOPT_MAX_HOST_CONNECTIONS, $this->maxThreads);
        }
    }

    /**
     * Validate and optimize configuration
     */
    private function validateConfig($config) {
        $required = ['accountName', 'apiToken', 'baseUrl', 'fields'];
        foreach ($required as $field) {
            if (empty($config[$field])) {
                throw new InvalidArgumentException("Missing required config: $field");
            }
        }

        $config['timeout'] = $config['timeout'] ?? self::DEFAULT_TIMEOUT;
        $config['connectTimeout'] = $config['connectTimeout'] ?? self::CONNECT_TIMEOUT;
        $config['maxRetries'] = $config['maxRetries'] ?? self::MAX_RETRIES;
        $config['rateLimit'] = $config['rateLimit'] ?? self::RATE_LIMIT_DELAY;
        $config['debug'] = $config['debug'] ?? false;

        return $config;
    }

    /**
     * Initialize directory structure
     */
    private function initializeDirectories() {
        $directories = [
            $this->downloadFolder,
            $this->downloadFolder . '/raw_data',
            $this->downloadFolder . '/processed_data',
            $this->downloadFolder . '/batch_results',
            $this->downloadFolder . '/exports',
            $this->downloadFolder . '/temp'
        ];

        foreach ($directories as $dir) {
            if (!is_dir($dir) && !mkdir($dir, 0755, true)) {
                throw new RuntimeException("Cannot create directory: $dir");
            }
        }
    }

    /**
     * Enhanced logging with batch processing
     */
    private function log($message, $level = 'INFO') {
        $timestamp = date('Y-m-d H:i:s');
        $logEntry = "[$timestamp] [$level] [BATCH {$this->currentBatch}] $message" . PHP_EOL;

        static $logBuffer = [];
        $logBuffer[] = $logEntry;

        if (count($logBuffer) >= 10 || $level === 'ERROR') {
            file_put_contents($this->logFile, implode('', $logBuffer), FILE_APPEND | LOCK_EX);
            file_put_contents($this->batchLogFile, implode('', $logBuffer), FILE_APPEND | LOCK_EX);
            $logBuffer = [];
        }

        echo $logEntry;

        if ($level === 'ERROR') {
            $this->stats['errors'][] = [
                'timestamp' => $timestamp,
                'message' => $message
            ];
        }
    }

    /**
     * Create optimized cURL handle
     */
    private function createOptimizedCurlHandle($url, $headers = [], $postData = null) {
        $ch = curl_init();

        curl_setopt_array($ch, [
            CURLOPT_URL => $url,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_HTTPHEADER => $headers,
            CURLOPT_TIMEOUT => $this->config['timeout'],
            CURLOPT_CONNECTTIMEOUT => $this->config['connectTimeout'],
            CURLOPT_SSL_VERIFYPEER => false,
            CURLOPT_SSL_VERIFYHOST => 0,
            CURLOPT_FOLLOWLOCATION => true,
            CURLOPT_MAXREDIRS => self::MAX_REDIRECTS,
            CURLOPT_TCP_NODELAY => true,
            CURLOPT_TCP_FASTOPEN => true,
            CURLOPT_BUFFERSIZE => self::CURL_BUFFER_SIZE,
            CURLOPT_DNS_CACHE_TIMEOUT => self::DNS_CACHE_TIMEOUT,
            CURLOPT_FRESH_CONNECT => false,
            CURLOPT_FORBID_REUSE => false,
            CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_2_0,
            CURLOPT_ENCODING => '',
            CURLOPT_USERAGENT => 'OptimizedPlanfixTaskDownloader/1.0',
        ]);

        if ($postData !== null) {
            curl_setopt($ch, CURLOPT_POST, true);
            curl_setopt($ch, CURLOPT_POSTFIELDS, $postData);
        }

        return $ch;
    }

    /**
     * Build filtered request body
     */
    private function buildFilteredRequest($offset = 0, $pageSize = null) {
        $pageSize = $pageSize ?? $this->batchSize;

        $requestBody = [
            'fields' => $this->config['fields'],
            'pageSize' => $pageSize,
            'offset' => $offset
        ];

        // Add filters if they exist
        if (!empty($this->filters)) {
            foreach ($this->filters as $key => $value) {
                if (!empty($value)) {
                    if ($key === 'filter_id') {
                        // Special handling for Planfix saved filters
                        $requestBody['filter'] = $value;
                    } else {
                        $requestBody[$key] = $value;
                    }
                }
            }
        }

        return $requestBody;
    }

    /**
     * High-speed parallel task requests
     */
    public function downloadTasksParallel($requestTasks) {
        if (empty($requestTasks)) {
            return [];
        }

        $results = [];
        $activeHandles = [];
        $taskQueue = array_values($requestTasks);
        $taskIndex = 0;

        $this->log("Starting parallel task download of " . count($requestTasks) . " requests with {$this->maxThreads} threads");
        $startTime = microtime(true);

        while ($taskIndex < count($taskQueue) || !empty($activeHandles)) {
            // Start new requests up to thread limit
            while (count($activeHandles) < $this->maxThreads && $taskIndex < count($taskQueue)) {
                $task = $taskQueue[$taskIndex];
                $requestId = $task['id'];

                $url = $this->config['baseUrl'] . $task['endpoint'];
                $headers = [
                    'Authorization: Bearer ' . $this->config['apiToken'],
                    'Content-Type: application/json',
                    'Accept: application/json',
                    'Connection: keep-alive'
                ];

                $postData = json_encode($task['data']);
                $ch = $this->createOptimizedCurlHandle($url, $headers, $postData);

                // Add to multi handle
                curl_multi_add_handle($this->curlMultiHandle, $ch);

                $activeHandles[$requestId] = [
                    'handle' => $ch,
                    'request_data' => $task,
                    'start_time' => microtime(true)
                ];

                $taskIndex++;
                $this->stats['concurrent_requests']++;
                $this->stats['peak_concurrent'] = max($this->stats['peak_concurrent'], count($activeHandles));
            }

            // Process completed requests
            $running = null;
            do {
                $status = curl_multi_exec($this->curlMultiHandle, $running);

                if ($status != CURLM_OK) {
                    break;
                }

                // Check for completed transfers
                while ($info = curl_multi_info_read($this->curlMultiHandle)) {
                    $ch = $info['handle'];
                    $requestId = null;

                    // Find which request this handle belongs to
                    foreach ($activeHandles as $id => $handleInfo) {
                        if ($handleInfo['handle'] === $ch) {
                            $requestId = $id;
                            break;
                        }
                    }

                    if ($requestId === null) continue;

                    $handleData = $activeHandles[$requestId];
                    $response = curl_multi_getcontent($ch);
                    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
                    $requestTime = microtime(true) - $handleData['start_time'];
                    $this->stats['total_request_time'] += $requestTime;

                    if ($info['result'] == CURLE_OK && $httpCode == 200) {
                        $decodedResponse = json_decode($response, true);

                        if (json_last_error() === JSON_ERROR_NONE) {
                            $this->stats['successful_requests']++;
                            $results[$requestId] = [
                                'success' => true,
                                'request_id' => $requestId,
                                'data' => $decodedResponse,
                                'request_time' => round($requestTime, 2),
                                'offset' => $handleData['request_data']['data']['offset'] ?? 0,
                                'pageSize' => $handleData['request_data']['data']['pageSize'] ?? 0
                            ];
                        } else {
                            $this->stats['failed_requests']++;
                            $results[$requestId] = [
                                'success' => false,
                                'request_id' => $requestId,
                                'error' => 'JSON decode error: ' . json_last_error_msg(),
                                'raw_response' => substr($response, 0, 500)
                            ];
                        }
                    } else {
                        $this->stats['failed_requests']++;
                        $results[$requestId] = [
                            'success' => false,
                            'request_id' => $requestId,
                            'error' => "Request failed: HTTP $httpCode, " . curl_error($ch),
                            'raw_response' => substr($response, 0, 500)
                        ];
                    }

                    // Cleanup
                    curl_multi_remove_handle($this->curlMultiHandle, $ch);
                    curl_close($ch);
                    unset($activeHandles[$requestId]);
                    $this->stats['concurrent_requests']--;
                }

                // Small delay to prevent CPU spinning
                if ($running > 0) {
                    curl_multi_select($this->curlMultiHandle, 0.1);
                }
            } while ($running > 0);
        }

        $totalTime = microtime(true) - $startTime;
        $this->log("Parallel requests completed in " . round($totalTime, 2) . "s. Peak concurrent: {$this->stats['peak_concurrent']}");

        return $results;
    }

    /**
     * Test API connection and get total task count
     */
    public function testConnectionAndGetCount() {
        $this->log("Testing API connection and getting task count...");

        try {
            $testData = $this->buildFilteredRequest(0, 1);
            $url = $this->config['baseUrl'] . '/task/list';

            $headers = [
                'Authorization: Bearer ' . $this->config['apiToken'],
                'Content-Type: application/json'
            ];

            $ch = $this->createOptimizedCurlHandle($url, $headers, json_encode($testData));
            $response = curl_exec($ch);
            $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            $error = curl_error($ch);
            curl_close($ch);

            if ($error || $httpCode !== 200) {
                throw new Exception("API connection failed: $error (HTTP $httpCode)");
            }

            $decodedResponse = json_decode($response, true);
            if (json_last_error() !== JSON_ERROR_NONE) {
                throw new Exception("JSON decode error: " . json_last_error_msg());
            }

            // Try to get total count from response
            $totalTasks = $decodedResponse['total'] ??
                         $decodedResponse['totalCount'] ??
                         $decodedResponse['count'] ??
                         'unknown';

            $this->log("API connection successful! Total tasks: $totalTasks");
            return [
                'success' => true,
                'total_tasks' => $totalTasks,
                'sample_response' => $decodedResponse
            ];

        } catch (Exception $e) {
            $this->log("API connection failed: " . $e->getMessage(), 'ERROR');
            return [
                'success' => false,
                'error' => $e->getMessage()
            ];
        }
    }

    /**
     * Process tasks in current batch with parallel requests
     */
    public function processBatch() {
        try {
            // Test connection first
            $connectionTest = $this->testConnectionAndGetCount();
            if (!$connectionTest['success']) {
                throw new Exception("API connection failed: " . $connectionTest['error']);
            }

            // Calculate batch range
            $startOffset = ($this->currentBatch - 1) * $this->batchSize;

            $this->log("Processing batch {$this->currentBatch}: offset $startOffset, size {$this->batchSize}");

            // Determine the correct endpoint based on filters
            $workingEndpoint = null;

            // Check if we're using a saved filter
            if (isset($this->filters['filter_id'])) {
                $filterId = $this->filters['filter_id'];
                $this->log("Using Planfix saved filter ID: $filterId");

                // Try filter-specific endpoints
                $filterEndpoints = [
                    "/task/list",
                    "/filter/$filterId/task",
                    "/filter/$filterId/tasks"
                ];

                foreach ($filterEndpoints as $endpoint) {
                    try {
                        $testData = $this->buildFilteredRequest(0, 1);
                        $url = $this->config['baseUrl'] . $endpoint;

                        $headers = [
                            'Authorization: Bearer ' . $this->config['apiToken'],
                            'Content-Type: application/json'
                        ];

                        $ch = $this->createOptimizedCurlHandle($url, $headers, json_encode($testData));
                        $response = curl_exec($ch);
                        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
                        curl_close($ch);

                        if ($httpCode == 200) {
                            $workingEndpoint = $endpoint;
                            $this->log("Found working filter endpoint: $endpoint");
                            break;
                        }
                    } catch (Exception $e) {
                        continue;
                    }
                }
            } else {
                // Use standard endpoints for other filters
                $endpoints = ['/task/list', '/task', '/tasks'];

                foreach ($endpoints as $endpoint) {
                    try {
                        $testData = $this->buildFilteredRequest(0, 1);
                        $url = $this->config['baseUrl'] . $endpoint;

                        $headers = [
                            'Authorization: Bearer ' . $this->config['apiToken'],
                            'Content-Type: application/json'
                        ];

                        $ch = $this->createOptimizedCurlHandle($url, $headers, json_encode($testData));
                        $response = curl_exec($ch);
                        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
                        curl_close($ch);

                        if ($httpCode == 200) {
                            $workingEndpoint = $endpoint;
                            $this->log("Found working endpoint: $endpoint");
                            break;
                        }
                    } catch (Exception $e) {
                        continue;
                    }
                }
            }

            if (!$workingEndpoint) {
                throw new Exception("No working API endpoints found for the specified filter");
            }

            // Create parallel request tasks
            $requestTasks = [];
            $totalRequests = min($this->maxThreads, ceil($this->batchSize / 100)); // Split into smaller chunks

            for ($i = 0; $i < $totalRequests; $i++) {
                $chunkOffset = $startOffset + ($i * 100);
                $chunkSize = min(100, $this->batchSize - ($i * 100));

                if ($chunkSize <= 0) break;

                $requestTasks[] = [
                    'id' => "request_$i",
                    'endpoint' => $workingEndpoint,
                    'data' => $this->buildFilteredRequest($chunkOffset, $chunkSize)
                ];
            }

            $this->log("Created " . count($requestTasks) . " parallel requests for batch");

            // Execute parallel requests
            $requestResults = $this->downloadTasksParallel($requestTasks);

            // Collect all tasks from responses
            $allTasks = [];
            foreach ($requestResults as $result) {
                if ($result['success']) {
                    $response = $result['data'];
                    $tasks = $response['tasks'] ??
                            $response['data'] ??
                            (is_array($response) ? $response : []);

                    if (is_array($tasks)) {
                        $allTasks = array_merge($allTasks, $tasks);
                    }
                }
            }

            $this->stats['tasks_in_batch'] = count($allTasks);
            $this->stats['processed_tasks'] = count($allTasks);
            $this->stats['total_tasks_downloaded'] = count($allTasks);

            $this->log("Collected " . count($allTasks) . " tasks from parallel requests");

            // Save batch results
            $this->saveBatchResults($allTasks, $requestResults);

            $this->stats['batch_end'] = date('Y-m-d H:i:s');

            $this->log("=" . str_repeat("=", 60));
            $this->log("Batch {$this->currentBatch} completed!");
            $this->log("Tasks downloaded: {$this->stats['total_tasks_downloaded']}");
            $this->log("Successful requests: {$this->stats['successful_requests']}");
            $this->log("Failed requests: {$this->stats['failed_requests']}");
            $this->log("Peak concurrent requests: {$this->stats['peak_concurrent']}");
            $this->log("Total request time: " . round($this->stats['total_request_time'], 2) . "s");
            $this->log("=" . str_repeat("=", 60));

            return [
                'success' => true,
                'tasks' => $allTasks,
                'stats' => $this->stats,
                'request_results' => $requestResults
            ];

        } catch (Exception $e) {
            $this->log("Critical error in batch processing: " . $e->getMessage(), 'ERROR');
            throw $e;
        }
    }

    /**
     * Save batch results in multiple formats
     */
    private function saveBatchResults($tasks, $requestResults) {
        $timestamp = date('Y-m-d_H-i-s');
        $batchPrefix = "batch_{$this->currentBatch}_{$timestamp}";

        // Save raw task data as JSON
        $rawDataFile = $this->downloadFolder . "/raw_data/{$batchPrefix}_tasks.json";
        file_put_contents($rawDataFile, json_encode($tasks, JSON_PRETTY_PRINT));
        $this->log("Raw task data saved: $rawDataFile");

        // Save as CSV
        if (!empty($tasks)) {
            $csvFile = $this->downloadFolder . "/exports/{$batchPrefix}_tasks.csv";
            $this->saveTasksAsCSV($tasks, $csvFile);
        }

        // Save batch summary
        $batchData = [
            'batch_number' => $this->currentBatch,
            'batch_start' => $this->stats['batch_start'],
            'batch_end' => $this->stats['batch_end'],
            'filters_applied' => $this->filters,
            'stats' => $this->stats,
            'request_details' => $requestResults,
            'task_count' => count($tasks),
            'sample_tasks' => array_slice($tasks, 0, 3) // First 3 tasks as sample
        ];

        $batchFile = $this->downloadFolder . "/batch_results/{$batchPrefix}_summary.json";
        file_put_contents($batchFile, json_encode($batchData, JSON_PRETTY_PRINT));
        $this->log("Batch summary saved: $batchFile");

        // Update global results
        $this->updateGlobalResults($tasks);
    }

    /**
     * Convert tasks to CSV format
     */
    private function saveTasksAsCSV($tasks, $filepath) {
        if (empty($tasks)) return;

        $fp = fopen($filepath, 'w');
        if (!$fp) {
            $this->log("Failed to create CSV file: $filepath", 'ERROR');
            return;
        }

        // Write UTF-8 BOM for Excel compatibility
        fwrite($fp, "\xEF\xBB\xBF");

        // Flatten first task to get headers
        $flattenedTask = $this->flattenTask($tasks[0]);
        $headers = array_keys($flattenedTask);
        fputcsv($fp, $headers);

        // Write data
        foreach ($tasks as $task) {
            $flattenedTask = $this->flattenTask($task);
            $row = [];
            foreach ($headers as $header) {
                $row[] = $flattenedTask[$header] ?? '';
            }
            fputcsv($fp, $row);
        }

        fclose($fp);
        $fileSize = filesize($filepath);
        $this->log("CSV saved: $filepath (" . $this->formatFileSize($fileSize) . ", " . count($tasks) . " rows)");
    }

    /**
     * Flatten task object for CSV export
     */
    private function flattenTask($task, $prefix = '') {
        $flattened = [];

        foreach ($task as $key => $value) {
            $newKey = $prefix ? "{$prefix}.{$key}" : $key;

            if ($value === null || $value === '') {
                $flattened[$newKey] = '';
            } elseif (is_array($value)) {
                if (empty($value)) {
                    $flattened[$newKey] = '';
                } else {
                    // Check if it's a simple array or complex
                    $firstItem = reset($value);
                    if (is_object($firstItem) || is_array($firstItem)) {
                        $flattened[$newKey] = json_encode($value);
                    } else {
                        $flattened[$newKey] = implode('; ', $value);
                    }
                }
            } elseif (is_object($value)) {
                $flattened = array_merge($flattened, $this->flattenTask($value, $newKey));
            } else {
                $flattened[$newKey] = $value;
            }
        }

        return $flattened;
    }

    /**
     * Update global results file
     */
    private function updateGlobalResults($newTasks) {
        $globalFile = $this->downloadFolder . '/all_tasks.json';

        $allTasks = [];
        if (file_exists($globalFile)) {
            $existingData = json_decode(file_get_contents($globalFile), true);
            if ($existingData && isset($existingData['tasks'])) {
                $allTasks = $existingData['tasks'];
            }
        }

        // Add new tasks (avoid duplicates by ID)
        $existingIds = array_column($allTasks, 'id');
        foreach ($newTasks as $task) {
            if (!in_array($task['id'], $existingIds)) {
                $allTasks[] = $task;
            }
        }

        $globalResults = [
            'updated_at' => date('Y-m-d H:i:s'),
            'total_tasks' => count($allTasks),
            'last_batch' => $this->currentBatch,
            'filters_applied' => $this->filters,
            'tasks' => $allTasks
        ];

        $tempFile = $globalFile . '.tmp';
        if (file_put_contents($tempFile, json_encode($globalResults, JSON_PRETTY_PRINT), LOCK_EX)) {
            rename($tempFile, $globalFile);
            $this->log("Global results updated: $globalFile (Total: " . count($allTasks) . " tasks)");
        }
    }

    /**
     * Format file size
     */
    private function formatFileSize($bytes) {
        $units = ['B', 'KB', 'MB', 'GB'];
        $bytes = max($bytes, 0);
        $pow = floor(($bytes ? log($bytes) : 0) / log(1024));
        $pow = min($pow, count($units) - 1);
        $bytes /= pow(1024, $pow);
        return round($bytes, 2) . ' ' . $units[$pow];
    }

    /**
     * Cleanup resources
     */
    public function __destruct() {
        if ($this->curlMultiHandle) {
            curl_multi_close($this->curlMultiHandle);
        }
    }

    /**
     * Get current stats
     */
    public function getStats() {
        return $this->stats;
    }
}

// Main execution
try {
    // Disable output buffering
    while (ob_get_level()) {
        ob_end_clean();
    }

    // Get parameters
    $batchNumber = 1;
    $threads = $defaultThreads;
    $filterPreset = 'all';

    if (isset($_GET['batch']) || php_sapi_name() !== 'cli') {
        // Web execution
        $batchNumber = isset($_GET['batch']) ? (int)$_GET['batch'] : 1;
        $threads = isset($_GET['threads']) ? max(1, min(20, (int)$_GET['threads'])) : $defaultThreads;
        $filterPreset = isset($_GET['filter']) ? $_GET['filter'] : 'all';

        set_time_limit(0);
        ignore_user_abort(true);
    } elseif (isset($argv[1])) {
        // Command line
        $batchNumber = (int)$argv[1];
        $threads = isset($argv[2]) ? max(1, min(20, (int)$argv[2])) : $defaultThreads;
        $filterPreset = isset($argv[3]) ? $argv[3] : 'all';
    }

    // Validate parameters
    if ($batchNumber < 1 || $batchNumber > $totalBatches) {
        $error = "Invalid batch number: $batchNumber. Must be between 1 and $totalBatches";
        file_put_contents($downloadFolder . '/error.log', date('Y-m-d H:i:s') . " - $error\n", FILE_APPEND | LOCK_EX);

        if (php_sapi_name() !== 'cli') {
            header('Content-Type: application/json');
            echo json_encode(['error' => $error, 'valid_range' => "1-$totalBatches"]);
        }
        exit(1);
    }

    // Get filters from preset
    $filters = $filterPresets[$filterPreset] ?? [];
    if (!isset($filterPresets[$filterPreset])) {
        $error = "Invalid filter preset: $filterPreset. Available: " . implode(', ', array_keys($filterPresets));
        file_put_contents($downloadFolder . '/error.log', date('Y-m-d H:i:s') . " - $error\n", FILE_APPEND | LOCK_EX);
        exit(1);
    }

    // Initialize downloader
    $downloader = new OptimizedPlanfixTaskDownloader(
        $planfixConfig,
        $downloadFolder,
        $batchSize,
        $batchNumber,
        $threads,
        $filters
    );

    // Process batch
    $results = $downloader->processBatch();

    // Log completion
    $statusData = [
        'batch' => $batchNumber,
        'threads' => $threads,
        'filter_preset' => $filterPreset,
        'status' => 'completed',
        'completed_at' => date('Y-m-d H:i:s'),
        'stats' => $downloader->getStats(),
        'execution_mode' => php_sapi_name() !== 'cli' ? 'web' : 'cli'
    ];

    file_put_contents($downloadFolder . "/task_batch_status.json", json_encode($statusData, JSON_PRETTY_PRINT), LOCK_EX);

    // Web response
    if (php_sapi_name() !== 'cli') {
        header('Content-Type: application/json');
        echo json_encode([
            'success' => true,
            'batch' => $batchNumber,
            'threads' => $threads,
            'filter_preset' => $filterPreset,
            'message' => "Task batch $batchNumber completed successfully",
            'stats' => $downloader->getStats(),
            'task_count' => $results['stats']['total_tasks_downloaded']
        ]);
    }

} catch (Exception $e) {
    $error = "Critical error: " . $e->getMessage();
    file_put_contents($downloadFolder . '/error.log', date('Y-m-d H:i:s') . " - $error\n", FILE_APPEND | LOCK_EX);

    if (php_sapi_name() !== 'cli') {
        header('Content-Type: application/json');
        http_response_code(500);
        echo json_encode(['error' => $error]);
    }
    exit(1);
}
?>
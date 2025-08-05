<?php

namespace Apps\Fintech\Packages\Mf\Extractdata;

use Apps\Fintech\Packages\Mf\Amcs\MfAmcs;
use Apps\Fintech\Packages\Mf\Categories\MfCategories;
use Apps\Fintech\Packages\Mf\Extractdata\Settings;
use Apps\Fintech\Packages\Mf\Schemes\MfSchemes;
use Apps\Fintech\Packages\Mf\Types\MfTypes;
use League\Csv\Reader;
use League\Csv\Statement;
use League\Flysystem\FilesystemException;
use League\Flysystem\UnableToCheckExistence;
use League\Flysystem\UnableToCreateDirectory;
use League\Flysystem\UnableToDeleteFile;
use League\Flysystem\UnableToDeleteDirectory;
use League\Flysystem\UnableToReadFile;
use League\Flysystem\UnableToWriteFile;
use Phalcon\Db\Enum;
use System\Base\BasePackage;
use System\Base\Providers\DatabaseServiceProvider\Sqlite;

class MfExtractdata extends BasePackage
{
    protected $now;

    protected $today;

    protected $year;

    protected $previousDay;

    protected $weekAgo;

    protected $sourceLink;

    protected $destDir = 'apps/Fintech/Packages/Mf/Extractdata/Data/';

    protected $destFile;

    protected $trackCounter = 0;

    public $method;

    protected $apiClient;

    protected $apiClientConfig;

    protected $settings = Settings::class;

    protected $navsPackage;

    protected $categoriesPackage;

    protected $amcsPackage;

    protected $schemesPackage;

    protected $mfFileSizeMatch = [];

    protected $schemes = [];

    protected $amcs = [];

    protected $categories = [];

    protected $parsedCarbon = [];

    public function onConstruct()
    {
        if (!is_dir(base_path($this->destDir))) {
            if (!mkdir(base_path($this->destDir), 0777, true)) {
                return false;
            }
        }

        //Increase Exectimeout to 5 hours as this process takes time to extract and merge data.
        if ((int) ini_get('max_execution_time') < 18000) {
            set_time_limit(18000);
        }

        //Increase memory_limit to 1G as the process takes a bit of memory to process the array.
        if ((int) ini_get('memory_limit') < 1024) {
            ini_set('memory_limit', '1024M');
        }

        $this->now = \Carbon\Carbon::now(new \DateTimeZone('Asia/Kolkata'));
        $this->year = $this->now->year;
        $this->today = $this->now->toDateString();
        $this->previousDay = $this->now->subDay(1)->toDateString();
        $this->now = $this->now->addDay(1);
        $this->weekAgo = $this->now->subDay(7)->toDateString();
        $this->now = $this->now->addDay(7);

        parent::onConstruct();
    }

    public function __call($method, $arguments)
    {
        if (method_exists($this, $method)) {
            if (PHP_SAPI !== 'cli') {
                $this->basepackages->progress->updateProgress($method, null, false);

                $call = call_user_func_array([$this, $method], $arguments);

                $callResult = $call;

                if ($call !== false) {
                    $call = true;
                }

                $this->basepackages->progress->updateProgress($method, $call, false);

                return $callResult;
            }

            call_user_func_array([$this, $method], $arguments);
        }
    }

    protected function downloadMfSchemesData()
    {
        $this->method = 'downloadMfSchemesData';

        $this->sourceLink = 'https://github.com/sp-fintech-mutualfunds/historical-mf-data/releases/latest/download/schemes.csv.zst';

        $this->destFile = base_path($this->destDir) . $this->today . '-schemes.csv.zst';

        try {
            //File is already downloaded
            if ($this->localContent->fileExists($this->destDir . $this->today . '-schemes.csv.zst')) {
                $remoteSize = (int) getRemoteFilesize($this->sourceLink);

                $localSize = $this->localContent->fileSize($this->destDir . $this->today . '-schemes.csv.zst');

                if ($remoteSize === $localSize) {
                    return true;
                }
            }
        } catch (FilesystemException | UnableToCheckExistence | UnableToRetrieveMetadata | \throwable $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }

        //Perform Old files cleanup
        if (!$this->cleanup('schemes')) {
            return false;
        }

        return $this->downloadData($this->sourceLink, $this->destFile);
    }

    protected function downloadMfNavsData($downloadLatestNav = true, $downloadAllNav = false)
    {
        $this->method = 'downloadMfNavsData';

        if ($downloadLatestNav) {
            try {
                if (!$this->localContent->fileExists($this->destDir . $this->year . '-funds.db.zst') &&
                    !$this->localContent->fileExists($this->destDir . $this->year . '-funds.db')
                ) {
                    $this->downloadMfNavsData(false, true);
                }

                $this->sourceLink = 'https://github.com/sp-fintech-mutualfunds/historical-mf-data/releases/latest/download/latest.db.zst';

                $this->destFile = base_path($this->destDir) . $this->today . '-latest.db.zst';

                //File is already downloaded
                if ($this->localContent->fileExists($this->destDir . $this->today . '-latest.db.zst')) {
                    $remoteSize = (int) getRemoteFilesize($this->sourceLink);

                    $localSize = $this->localContent->fileSize($this->destDir . $this->today . '-latest.db.zst');

                    if ($remoteSize === $localSize) {
                        $this->mfFileSizeMatch['latest'] = true;

                        return true;
                    }
                }
            } catch (FilesystemException | UnableToCheckExistence | UnableToDeleteFile | UnableToRetrieveMetadata | \throwable $e) {
                $this->addResponse($e->getMessage(), 1);

                return false;
            }

            //Perform Old files cleanup
           if (!$this->cleanup('-latest')) {
                return false;
            }
        }

        if ($downloadAllNav) {
            $this->sourceLink = 'https://github.com/sp-fintech-mutualfunds/historical-mf-data/releases/latest/download/funds.db.zst';

            $this->destFile = base_path($this->destDir) . $this->year . '-funds.db.zst';

            try {
                //File is already downloaded
                if ($this->localContent->fileExists($this->destDir . $this->year . '-funds.db.zst')) {
                    return true;
                    // $remoteSize = (int) getRemoteFilesize($this->sourceLink);

                    // $localSize = $this->localContent->fileSize($this->destDir . $this->year . '-funds.db.zst');

                    // if ($remoteSize === $localSize) {
                    //     $this->mfFileSizeMatch['funds'] = true;

                    //     return true;
                    // }
                }
            } catch (FilesystemException | UnableToCheckExistence | UnableToDeleteFile | UnableToRetrieveMetadata | \throwable $e) {
                $this->addResponse($e->getMessage(), 1);

                return false;
            }

            //Perform Old files cleanup
           if (!$this->cleanup('-funds')) {
                return false;
            }
        }

        return $this->downloadData($this->sourceLink, $this->destFile);
    }

    protected function downloadData($url, $sink)
    {
        $download = $this->remoteWebContent->request(
            'GET',
            $url,
            [
                'progress' => function(
                    $downloadTotal,
                    $downloadedBytes,
                    $uploadTotal,
                    $uploadedBytes
                ) {
                    $counters =
                            [
                                'downloadTotal'     => $downloadTotal,
                                'downloadedBytes'   => $downloadedBytes,
                                'uploadTotal'       => $uploadTotal,
                                'uploadedBytes'     => $uploadedBytes
                            ];

                    if ($downloadedBytes === 0) {
                        return;
                    }

                    //Trackcounter is needed as guzzelhttp runs this in a while loop causing too many updates with same download count.
                    //So this way, we only update progress when there is actually an update.
                    if ($downloadedBytes === $this->trackCounter) {
                        return;
                    }

                    $this->trackCounter = $downloadedBytes;

                    if (PHP_SAPI !== 'cli') {
                        if ($downloadedBytes === $downloadTotal) {
                            $this->basepackages->progress->updateProgress($this->method, true, false, null, $counters);
                        } else {
                            $this->basepackages->progress->updateProgress($this->method, null, false, null, $counters);
                        }
                    }
                },
                'verify'            => false,
                'connect_timeout'   => 5,
                'sink'              => $sink
            ]
        );

        $this->trackCounter = 0;

        if ($download->getStatusCode() === 200) {
            return true;
        }

        $this->addResponse('Download resulted in : ' . $download->getStatusCode(), 1);

        return false;
    }

    protected function cleanup($type)
    {
        try {
            $scanDir = $this->basepackages->utils->scanDir($this->destDir, false);

            if ($scanDir && count($scanDir['files']) > 0) {
                foreach ($scanDir['files'] as $file) {
                    if ($type === '-funds') {
                        if (!str_starts_with($file, $this->year) &&
                            str_contains($file, $type)
                        ) {
                            $this->localContent->delete($file);
                        }
                    } else {
                        if (!str_starts_with($file, $this->today) &&
                            str_contains($file, $type)
                        ) {
                            $this->localContent->delete($file);
                        }
                    }
                }
            }
        } catch (UnableToDeleteFile | \throwable | FilesystemException $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }

        return true;
    }

    protected function extractMfSchemesData()
    {
        try {
            //Decompress
            exec('unzstd -d -f ' . base_path($this->destDir) . $this->today . '-schemes.csv.zst -o ' . base_path($this->destDir) . $this->today . '-schemes.csv', $output, $result);

            if ($result !== 0) {
                return $this->extractionFail($output);
            }

            return true;
        } catch (\throwable $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }
    }

    protected function extractMfNavsData($extractLatestNav = true)
    {
        $this->method = 'extractMfNavsData';

        $files = [];

        if ($extractLatestNav) {
            if ($this->localContent->fileExists($this->destDir . $this->today . '-latest.db.zst')) {
                array_push($files, '-latest');
            }
        }

        if ($this->localContent->fileExists($this->destDir . $this->year . '-funds.db.zst') &&
            !$this->localContent->fileExists($this->destDir . $this->year . '-funds.db')
        ) {
            array_push($files, '-funds');
        }

        if (count($files) === 0) {
            $this->addResponse('Nothing to extract!', 1);

            return false;
        }

        foreach ($files as $file) {
            try {
                if (str_contains($file, '-latest')) {
                    if ($this->localContent->fileExists($this->destDir . $this->today . $file . '.db')) {
                        if (isset($this->mfFileSizeMatch) &&
                            $this->mfFileSizeMatch['latest'] === true
                        ) {//If compressed file match, the decompressed and indexed will also match.
                            continue;
                        }
                    }

                    $this->localContent->delete($this->destDir . $this->today . $file . '.db');

                    $file = $this->today . $file;
                } else if (str_contains($file, '-funds')) {
                    // if (isset($this->mfFileSizeMatch) &&
                    //     $this->mfFileSizeMatch['funds'] === true
                    // ) {//If compressed file match, the decompressed and indexed will also match.
                    //     continue;
                    // }

                    $this->localContent->delete($this->destDir . $this->year . $file . '.db');

                    $file = $this->year . $file;
                }

                //Decompress
                exec('unzstd -d -f ' . base_path($this->destDir) . $file . '.db.zst -o ' . base_path($this->destDir) . $file . '.db', $output, $result);
                if (PHP_SAPI !== 'cli') {
                    $this->basepackages->progress->updateProgress(
                        method: $this->method,
                        counters: ['stepsTotal' => 5, 'stepsCurrent' => 1],
                        text: 'Decompressing...'
                    );
                }

                if ($result !== 0) {
                    return $this->extractionFail($output);
                }

                //Create INDEXES
                exec("echo 'CREATE INDEX \"nav-main\" ON \"nav\" (\"date\",\"scheme_code\")' | sqlite3 " . base_path($this->destDir) . $file . ".db", $output, $result);
                if (PHP_SAPI !== 'cli') {
                    $this->basepackages->progress->updateProgress(
                        method: $this->method,
                        counters: ['stepsTotal' => 5, 'stepsCurrent' => 2],
                        text: 'Generating Main Index...'
                    );
                }

                if ($result !== 0) {
                    return $this->indexingFail($output);
                }

                exec("echo 'CREATE INDEX \"nav-scheme\" ON \"nav\" (\"scheme_code\")' | sqlite3 " . base_path($this->destDir) . $file . ".db", $output, $result);
                if (PHP_SAPI !== 'cli') {
                    $this->basepackages->progress->updateProgress(
                        method: $this->method,
                        counters: ['stepsTotal' => 5, 'stepsCurrent' => 3],
                        text: 'Generating Scheme Index...'
                    );
                }

                if ($result !== 0) {
                    return $this->indexingFail($output);
                }

                exec("echo 'CREATE INDEX \"securities-scheme\" ON \"securities\" (\"scheme_code\")' | sqlite3 " . base_path($this->destDir) . $file . ".db", $output, $result);
                if (PHP_SAPI !== 'cli') {
                    $this->basepackages->progress->updateProgress(
                        method: $this->method,
                        counters: ['stepsTotal' => 5, 'stepsCurrent' => 4],
                        text: 'Generating Securities Scheme Index...'
                    );
                }

                if ($result !== 0) {
                    return $this->indexingFail($output);
                }

                exec("echo 'CREATE INDEX \"securities-isin\" ON \"securities\" (\"isin\")' | sqlite3 " . base_path($this->destDir) . $file . ".db", $output, $result);
                if (PHP_SAPI !== 'cli') {
                    $this->basepackages->progress->updateProgress(
                        method: $this->method,
                        counters: ['stepsTotal' => 5, 'stepsCurrent' => 5],
                        text: 'Generating Securities Isin Index...'
                    );
                }

                if ($result !== 0) {
                    return $this->indexingFail($output);
                }
            } catch (FilesystemException | UnableToCheckExistence | \throwable $e) {
                $this->addResponse($e->getMessage(), 1);

                return false;
            }
        }

        return true;
    }

    protected function extractionFail($output)
    {
        $this->addResponse('Error extracting file', 1, ['output' => $output]);

        return false;
    }

    protected function indexingFail($output)
    {
        $this->addResponse('Error indexing file', 1, ['output' => $output]);

        return false;
    }

    protected function processMfSchemesData()
    {
        $this->method = 'processMfSchemesData';

        try {
            $csv = Reader::createFromStream($this->localContent->readStream($this->destDir . $this->today . '-schemes.csv'));
            $csv->setHeaderOffset(0);

            $statement = (new Statement())->orderByAsc('AMC');
            $records = $statement->process($csv);

            $isinsTotal = count($records);
            $lineNo = 1;

            foreach ($records as $line) {
                $store = 'apps_fintech_mf_schemes';
                $moved = false;

                //Timer
                $this->basepackages->utils->setMicroTimer('Start');

                if (strlen($line['ISIN Div Payout/ ISIN GrowthISIN Div Reinvestment']) === 0) {
                    continue;
                }

                $isinArr = explode('INF', $line['ISIN Div Payout/ ISIN GrowthISIN Div Reinvestment']);

                if (count($isinArr) === 1 &&
                    ($isinArr[0] === '' ||
                     str_starts_with($isinArr[0], 'xxxxxx') ||
                     $isinArr[0] === $line['ISIN Div Payout/ ISIN GrowthISIN Div Reinvestment'])
                ) {
                    continue;
                }

                $schemeName = strtolower($line['Scheme NAV Name']);
                if ((str_contains($schemeName, 'regular') ||
                    str_contains($schemeName, 'idcw') ||
                    str_contains($schemeName, 'dividend') ||
                    str_contains($schemeName, 'etf') ||
                    str_contains($schemeName, 'income distribution')) &&
                    !str_contains($schemeName, 'direct')
                ) {//For personal use, we dont want regular expense ratio type.
                    $store = 'apps_fintech_mf_schemes_all';
                }

                try {
                    $scheme = null;

                    if ($this->localContent->fileExists('.ff/sp/' . $store . '/data/' . $line['Code'] . '.json')) {
                        $scheme = $this->helper->decode($this->localContent->read('.ff/sp/' . $store . '/data/' . $line['Code'] . '.json'), true);
                    }
                    if ($this->localContent->fileExists('.ff/sp/apps_fintech_mf_schemes/data/' . $line['Code'] . '.json')) {
                        $scheme = $this->helper->decode($this->localContent->read('.ff/sp/' . $store . '/data/' . $line['Code'] . '.json'), true);
                        $store = 'apps_fintech_mf_schemes';
                        $moved = true;
                    }
                } catch (FilesystemException | UnableToReadFile | UnableToCheckExistence | \throwable $e) {
                    $this->addResponse($e->getMessage(), 1);

                    return false;
                }

                if ($scheme) {
                    if ($scheme['id'] == $line['Code']) {
                        // if (!isset($scheme['navs_last_updated']) && $store === 'apps_fintech_mf_schemes') {
                        //     try {
                        //         $dbNav = null;

                        //         if ($this->localContent->fileExists('.ff/sp/apps_fintech_mf_schemes_navs/data/' . $scheme['id'] . '.json')) {
                        //             $dbNav = $this->helper->decode($this->localContent->read('.ff/sp/apps_fintech_mf_schemes_navs/data/' . $scheme['id'] . '.json'), true);
                        //         }
                        //     } catch (FilesystemException | UnableToReadFile | UnableToCheckExistence | \throwable $e) {
                        //         $this->addResponse($e->getMessage(), 1);

                        //         return false;
                        //     }

                        //     if ($dbNav && isset($dbNav['last_updated'])) {
                        //         $scheme['navs_last_updated'] = $dbNav['last_updated'];

                        //         if ($this->config->databasetype === 'db') {
                        //             $this->db->insertAsDict('apps_fintech_mf_schemes', $scheme);//This also needs update.
                        //         } else {
                        //             try {
                        //                 $this->localContent->write('.ff/sp/apps_fintech_mf_schemes/data/' . $line['Code'] . '.json', $this->helper->encode($scheme));
                        //             } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                        //                 $this->addResponse($e->getMessage(), 1);

                        //                 return false;
                        //             }
                        //         }
                        //     }
                        // }

                        $lineMd5 = hash('md5', implode(',', $line));

                        if ($scheme['scheme_md5'] &&
                            $scheme['scheme_md5'] === $lineMd5
                        ) {
                            $this->processUpdateTimer($isinsTotal, $lineNo);

                            $lineNo++;

                            continue;
                        }
                    } else {
                        //We found a duplicate entry in the CSV file with different amfi_code
                        if (count($isinArr) === 2) {
                            continue;
                        } else if (count($isinArr) === 3) {
                            continue;
                        }
                    }
                } else {
                    $scheme = [];
                }

                $amc = $this->processAmcs($line);
                if (!$amc) {
                    $this->basepackages->progress->setErrors([
                        'error' => 'Cannot create new AMC information for line# ' . $lineNo,
                        'line' => $this->helper->encode($line)
                    ]);

                    $this->addResponse('Cannot create new AMC information for line# ' . $lineNo, 1, ['line' => $this->helper->encode($line)]);

                    return false;
                }

                $category = $this->processCategories($line);
                if (!$category) {
                    $this->basepackages->progress->setErrors([
                        'error' => 'Cannot create new category information for line# ' . $lineNo,
                        'line' => $this->helper->encode($line)
                    ]);

                    $this->addResponse('Cannot create new category information for line# ' . $lineNo, 1, ['line' => $this->helper->encode($line)]);

                    return false;
                }

                if (count($isinArr) === 2) {
                    $scheme['isin'] = 'INF' . trim($isinArr[1]);
                } else if (count($isinArr) === 3) {
                    $scheme['isin'] = 'INF' . trim($isinArr[1]);
                    $scheme['isin_reinvest'] = 'INF' . trim($isinArr[2]);
                }

                $scheme['amc_id'] = $amc['id'];
                $scheme['id'] = (int) $line['Code'];
                $scheme['amfi_code'] = $line['Code'];
                $scheme['scheme_type'] = $line['Scheme Type'];
                $scheme['category_id'] = $category['id'];
                $scheme['name'] = $line['Scheme NAV Name'];
                $scheme['scheme_name'] = $line['Scheme Name'];
                $scheme['launch_date'] = null;
                $scheme['latest_nav'] = 0;
                if ($line['Launch Date'] !== '') {
                    if (!isset($this->parsedCarbon[$line['Launch Date']])) {
                        $this->parsedCarbon[$line['Launch Date']] = \Carbon\Carbon::parse($line['Launch Date']);
                    }

                    $scheme['launch_date'] = $this->parsedCarbon[$line['Launch Date']]->toDateString();
                }
                $scheme['minimum_amount'] = null;
                if ($line['Scheme Minimum Amount'] !== '') {
                    $scheme['minimum_amount'] = $line['Scheme Minimum Amount'];
                }

                if (str_contains($schemeName, 'direct')) {
                    $scheme['expense_ratio_type'] = 'Direct';
                } else {
                    if (!$moved) {
                        $store = 'apps_fintech_mf_schemes_all';
                    }
                }
                if (str_contains($schemeName, 'growth')) {
                    $scheme['plan_type'] = 'Growth';
                } else {
                    if (!$moved) {
                        $store = 'apps_fintech_mf_schemes_all';
                    }
                }
                if (str_contains($schemeName, 'passive')) {
                    $scheme['management_type'] = 'Passive';
                } else {
                    $scheme['management_type'] = 'Active';
                }

                $scheme['scheme_md5'] = hash('md5', implode(',', $line));
                $scheme['navs_last_updated'] = null;

                if ($this->config->databasetype === 'db') {
                    $this->db->insertAsDict($store, $scheme);//This also needs update.
                } else {
                    if ($store === 'apps_fintech_mf_schemes') {//Insert also in  apps_fintech_mf_schemes_all
                        try {
                            $this->localContent->write('.ff/sp/apps_fintech_mf_schemes_all/data/' . $line['Code'] . '.json', $this->helper->encode($scheme));
                        } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                            $this->addResponse($e->getMessage(), 1);

                            return false;
                        }
                    }

                    try {
                        $this->localContent->write('.ff/sp/' . $store . '/data/' . $line['Code'] . '.json', $this->helper->encode($scheme));
                    } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                        $this->addResponse($e->getMessage(), 1);

                        return false;
                    }

                    try {
                        if (!$this->localContent->directoryExists($this->destDir . 'navsindex/' . $line['Code'])) {
                            $this->localContent->createDirectory($this->destDir . 'navsindex/' . $line['Code']);
                        }
                    } catch (FilesystemException | UnableToCheckExistence | UnableToCreateDirectory | \throwable $e) {
                        $this->addResponse($e->getMessage(), 1);

                        return false;
                    }
                }

                //Timer
                $this->processUpdateTimer($isinsTotal, $lineNo);

                $lineNo++;
            }
        } catch (\throwable $e) {
            $errors['exception'] = $e->getMessage();

            if (isset($lineNo)) {
                $errors['lineNo'] = $lineNo;
            }
            if (isset($line)) {
                $errors['line'] = $this->helper->encode($line);
            }
            if (isset($lineMd5)) {
                $errors['lineMd5'] = $lineMd5;
            }
            if (isset($scheme)) {
                $errors['scheme'] = $this->helper->encode($scheme);
            }

            $this->addResponse($e->getMessage(), 1, ['errors' => $errors]);

            $this->basepackages->progress->setErrors($errors);

            $this->basepackages->progress->resetProgress();

            throw $e;
        }

        return true;
    }

    protected function processMfNavsData($data = [])
    {
        $this->method = 'processMfNavsData';

        try {
            if (!$this->localContent->fileExists($this->destDir . $this->today . '-latest.db')) {
                $this->addResponse('Latest navs file does not exists. Please extract data first.', 1);

                return false;
            }

            $sqliteLatest = (new Sqlite())->init(base_path($this->destDir . $this->today . '-latest.db'));

            $sqlite = (new Sqlite())->init(base_path($this->destDir . $this->year . '-funds.db'));
        } catch (FilesystemException | UnableToReadFile | UnableToCheckExistence | \throwable $e) {
            trace([$e]);
            $this->addResponse($e->getMessage(), 1);

            return false;
        }

        try {
            if (isset($data['scheme_id'])) {
                try {
                    if ($this->localContent->fileExists('.ff/sp/apps_fintech_mf_schemes/data/' . $data['scheme_id'] . '.json')) {
                        $this->schemes = [$this->helper->decode($this->localContent->read('.ff/sp/apps_fintech_mf_schemes/data/' . $data['scheme_id'] . '.json'), true)];

                        $dbCount = 1;
                    } else {
                        if (isset($data['force']) && $data['force'] == 'true') {
                            if ($this->localContent->fileExists('.ff/sp/apps_fintech_mf_schemes_all/data/' . $data['scheme_id'] . '.json')) {
                                $this->schemes = [$this->helper->decode($this->localContent->read('.ff/sp/apps_fintech_mf_schemes_all/data/' . $data['scheme_id'] . '.json'), true)];

                                $dbCount = 1;
                            } else {
                                $this->addResponse('Scheme with ID does not exists', 1);

                                return false;
                            }
                        } else {
                            $this->addResponse('Scheme with ID does not exists', 1);

                            return false;
                        }
                    }
                } catch (FilesystemException | UnableToReadFile | UnableToCheckExistence | \throwable $e) {
                    $this->addResponse($e->getMessage(), 1);

                    return false;
                }
            } else {
                if (!$this->schemesPackage) {
                    $this->schemesPackage = $this->usePackage(MfSchemes::class);
                }

                $this->schemes = $this->schemesPackage->getAll()->mfschemes;

                $dbCount = count($this->schemes);

                if ($dbCount === 0) {
                    $this->addResponse('No Schemes found, Import schemes data first.', 1);

                    return false;
                }
            }

            if (count($this->schemes) > 1) {
                $this->schemes = msort($this->schemes, 'id');
            }

            //To reimport everything!! Comment if not used.
            // $data['get_all_navs'] = true;

            for ($i = 0; $i < $dbCount; $i++) {
                $this->basepackages->utils->setMicroTimer('Start');

                $amfiNavsArr = array_merge(
                    $sqlite->query(
                    "SELECT * from nav N
                    JOIN securities S ON N.scheme_code = S.scheme_code
                    WHERE S.scheme_code = '" . $this->schemes[$i]['id'] . "' AND S.type = '0'
                    AND N.date >= 2000-01-01
                    ORDER BY N.date ASC"
                    )->fetchAll(Enum::FETCH_ASSOC),
                    $sqliteLatest->query(
                        "SELECT * from nav N
                        JOIN securities S ON N.scheme_code = S.scheme_code
                        WHERE S.scheme_code = '" . $this->schemes[$i]['id'] . "' AND S.type = '0'
                        AND N.date >= 2000-01-01
                        ORDER BY N.date ASC"
                    )->fetchAll(Enum::FETCH_ASSOC)
                );

                if (!$amfiNavsArr ||
                    ($amfiNavsArr && count($amfiNavsArr) === 0)
                ) {
                    $amfiNavsArr = array_merge(
                        $sqlite->query(
                        "SELECT * from nav N
                        JOIN securities S ON N.scheme_code = S.scheme_code
                        WHERE S.scheme_code = '" . $this->schemes[$i]['id'] . "' AND S.type = '1'
                        AND N.date >= 2000-01-01
                        ORDER BY N.date ASC"
                        )->fetchAll(Enum::FETCH_ASSOC),
                        $sqliteLatest->query(
                            "SELECT * from nav N
                            JOIN securities S ON N.scheme_code = S.scheme_code
                            WHERE S.scheme_code = '" . $this->schemes[$i]['id'] . "' AND S.type = '1'
                            AND N.date >= 2000-01-01
                            ORDER BY N.date ASC"
                        )->fetchAll(Enum::FETCH_ASSOC)
                    );

                    if (!$amfiNavsArr ||
                        ($amfiNavsArr && count($amfiNavsArr) === 0)
                    ) {
                        try {
                            if ($this->localContent->fileExists('.ff/sp/apps_fintech_mf_schemes/data/' . $this->schemes[$i]['id'] . '.json')) {
                                $this->localContent->delete('.ff/sp/apps_fintech_mf_schemes/data/' . $this->schemes[$i]['id'] . '.json');

                                if (!$this->localContent->directoryExists($this->destDir . 'navsindex/' . $this->schemes[$i]['id'])) {
                                    $this->localContent->deleteDirectory($this->destDir . 'navsindex/' . $this->schemes[$i]['id']);
                                }

                                unset($this->schemes[$this->schemes[$i]['id']]);

                                $this->processUpdateTimer($dbCount, $i + 1);

                                continue;
                            }
                        } catch (FilesystemException | UnableToDeleteFile | UnableToDeleteDirectory | UnableToCheckExistence | \throwable $e) {
                            $this->addResponse($e->getMessage(), 1);

                            return false;
                        }
                    }
                }

                if (count($amfiNavsArr) <= 2) {
                    if ((isset($amfiNavsArr[1]['date']) && $amfiNavsArr[0]['date'] === $amfiNavsArr[1]['date']) ||
                        count($amfiNavsArr) === 1
                    ) {
                        try {
                            if ($this->localContent->fileExists('.ff/sp/apps_fintech_mf_schemes/data/' . $this->schemes[$i]['id'] . '.json')) {
                                $this->localContent->delete('.ff/sp/apps_fintech_mf_schemes/data/' . $this->schemes[$i]['id'] . '.json');

                                if (!$this->localContent->directoryExists($this->destDir . 'navsindex/' . $this->schemes[$i]['id'])) {
                                    $this->localContent->deleteDirectory($this->destDir . 'navsindex/' . $this->schemes[$i]['id']);
                                }

                                unset($this->schemes[$this->schemes[$i]['id']]);

                                $this->processUpdateTimer($dbCount, $i + 1);

                                continue;
                            }
                        } catch (FilesystemException | UnableToDeleteFile | UnableToDeleteDirectory | UnableToCheckExistence | \throwable $e) {
                            $this->addResponse($e->getMessage(), 1);

                            return false;
                        }
                    }
                }

                if (!isset($this->schemes[$i]['start_date']) ||
                    (isset($this->schemes[$i]['start_date']) && $this->schemes[$i]['start_date'] != $this->helper->first($amfiNavsArr)['date'])
                ) {
                    $this->schemes[$i]['start_date'] = $this->helper->first($amfiNavsArr)['date'];

                    try {
                        $this->localContent->write('.ff/sp/apps_fintech_mf_schemes/data/' . $this->schemes[$i]['id'] . '.json', $this->helper->encode($this->schemes[$i]));
                    } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                        $this->addResponse($e->getMessage(), 1);

                        return false;
                    }
                }

                $amfiNavs = [];

                for ($amfiNavsArrKey = 0; $amfiNavsArrKey < count($amfiNavsArr); $amfiNavsArrKey++) {
                    $amfiNavs[$amfiNavsArr[$amfiNavsArrKey]['date']] = $amfiNavsArr[$amfiNavsArrKey];
                }

                $dbNav = null;

                try {
                    if ($this->localContent->fileExists('.ff/sp/apps_fintech_mf_schemes_navs/data/' . $this->schemes[$i]['id'] . '.json')) {
                        $dbNav = $this->helper->decode($this->localContent->read('.ff/sp/apps_fintech_mf_schemes_navs/data/' . $this->schemes[$i]['id'] . '.json'), true);
                    }
                } catch (FilesystemException | UnableToReadFile | UnableToCheckExistence | \throwable $e) {
                    $this->addResponse($e->getMessage(), 1);

                    return false;
                }

                //if in case you just want to extract Navs data from the Navs DB and dump individual navs data in Extractdata/Data/navs folder
                if ($dbNav && $dbNav['navs'] && count($dbNav['navs']) > 0
                    && isset($data['navsindex'])
                ) {
                    foreach ($dbNav['navs'] as $dbNavDate => $dbNavNavs) {
                        $dates = explode('-', $dbNavDate);

                        try {
                            if (!$this->localContent->directoryExists($this->destDir . 'navsindex/' . $this->schemes[$i]['id'] . '/' . $dates[0])) {
                                $this->localContent->createDirectory($this->destDir . 'navsindex/' . $this->schemes[$i]['id'] . '/' . $dates[0]);
                            }
                            if (!$this->localContent->directoryExists($this->destDir . 'navsindex/' . $this->schemes[$i]['id'] . '/' . $dates[0] . '/' . $dates[1])) {
                                $this->localContent->createDirectory($this->destDir . 'navsindex/' . $this->schemes[$i]['id'] . '/' . $dates[0] . '/' . $dates[1]);
                            }

                            $this->localContent->write(
                                $this->destDir . 'navsindex/' . $this->schemes[$i]['id'] . '/' . $dates[0] . '/' . $dates[1] . '/' . $dates[2] . '.json', $this->helper->encode($dbNavNavs)
                            );
                        } catch (FilesystemException | UnableToCheckExistence | UnableToCreateDirectory | \throwable $e) {
                            $this->addResponse($e->getMessage(), 1);

                            return false;
                        }
                    }

                    $this->processUpdateTimer($dbCount, $i + 1);

                    continue;
                }

                if ($dbNav && $dbNav['navs'] && count($dbNav['navs']) > 0 &&
                    isset($dbNav['last_updated']) &&
                    !isset($data['get_all_navs']) &&
                    $this->helper->last($amfiNavs)['date'] === $dbNav['last_updated']
                ) {
                    $this->processUpdateTimer($dbCount, $i + 1);

                    continue;
                }

                if (!$dbNav) {
                    $dbNav = [];
                    $dbNav['id'] = (int) $this->schemes[$i]['id'];
                    $dbNav['navs'] = [];
                } else {
                    if (isset($data['get_all_navs']) && $data['get_all_navs'] == 'true') {
                        $dbNav['navs'] = [];
                    }
                }

                $firstAmfiNavs = $this->helper->first($amfiNavs);

                $dbNav['last_updated'] = $this->helper->last($amfiNavs)['date'];

                $newNavs = false;

                if (isset($data['get_all_navs']) && $data['get_all_navs'] == 'true') {
                    $amfiNavs = array_values($amfiNavs);
                } else {
                    if (count($dbNav['navs']) > 0) {
                        $amfiNavsKeysDiff = array_diff(array_keys($amfiNavs), array_keys($dbNav['navs']));

                        if (count($amfiNavsKeysDiff) > 0) {
                            $amfiNavs = array_values(array_slice($amfiNavs, $this->helper->firstKey($amfiNavsKeysDiff) - 2));//Get previous day for diff

                            $newNavs = [];
                        }
                    } else {
                        $amfiNavs = array_values($amfiNavs);
                    }
                }

                $amfiNavs = $this->fillAmfiNavDays($amfiNavs, $this->schemes[$i]['id']);

                if (count($amfiNavs) === 0) {
                    $this->processUpdateTimer($dbCount, $i + 1);

                    continue;
                }

                foreach ($amfiNavs as $amfiNavKey => $amfiNav) {
                    if (!isset($dbNav['navs'][$amfiNav['date']])) {
                        $dbNav['navs'][$amfiNav['date']]['nav'] = $amfiNav['nav'];
                        $dbNav['navs'][$amfiNav['date']]['date'] = $amfiNav['date'];
                        if (!isset($this->parsedCarbon[$amfiNav['date']])) {
                            $this->parsedCarbon[$amfiNav['date']] = \Carbon\Carbon::parse($amfiNav['date']);
                        }

                        $dbNav['navs'][$amfiNav['date']]['timestamp'] = $this->parsedCarbon[$amfiNav['date']]->timestamp;

                        if ($amfiNavKey !== 0) {
                            $previousDay = $amfiNavs[$amfiNavKey - 1];

                            $dbNav['navs'][$amfiNav['date']]['diff'] =
                                numberFormatPrecision($amfiNav['nav'] - $previousDay['nav'], 4);
                            $dbNav['navs'][$amfiNav['date']]['diff_percent'] =
                                numberFormatPrecision(($amfiNav['nav'] * 100 / $previousDay['nav']) - 100, 2);

                            $dbNav['navs'][$amfiNav['date']]['trajectory'] = '-';
                            if ($amfiNav['nav'] > $previousDay['nav']) {
                                $dbNav['navs'][$amfiNav['date']]['trajectory'] = 'up';
                            } else {
                                $dbNav['navs'][$amfiNav['date']]['trajectory'] = 'down';
                            }

                            $dbNav['navs'][$amfiNav['date']]['diff_since_inception'] =
                                numberFormatPrecision($amfiNav['nav'] - $firstAmfiNavs['nav'], 4);
                            $dbNav['navs'][$amfiNav['date']]['diff_percent_since_inception'] =
                                numberFormatPrecision(($amfiNav['nav'] * 100 / $firstAmfiNavs['nav'] - 100), 2);
                        }

                        if ($newNavs !== false) {
                            $newNavs[$amfiNav['date']] = $dbNav['navs'][$amfiNav['date']];
                        }
                    }
                }

                $dbNav['navs'] = msort(array: $dbNav['navs'], key: 'timestamp', preserveKey: true);
                $newNavs = msort(array: $newNavs, key: 'timestamp', preserveKey: true);

                if (!$this->createChunks($dbNav, $data, $newNavs)) {
                    return false;
                }

                if (!$this->createRollingReturns($dbNav, $i, $data, $newNavs)) {
                    return false;
                }

                if ($this->config->databasetype === 'db') {
                    $this->db->insertAsDict('apps_fintech_mf_navs', $dbNav);
                } else {
                    try {
                        $this->localContent->write('.ff/sp/apps_fintech_mf_schemes_navs/data/' . $this->schemes[$i]['id'] . '.json', $this->helper->encode($dbNav));
                    } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                        $this->addResponse($e->getMessage(), 1);

                        return false;
                    }

                    foreach ($dbNav['navs'] as $dbNavDate => $dbNavNavs) {
                        $dates = explode('-', $dbNavDate);

                        try {
                            if (!$this->localContent->directoryExists($this->destDir . 'navsindex/' . $this->schemes[$i]['id'] . '/' . $dates[0])) {
                                $this->localContent->createDirectory($this->destDir . 'navsindex/' . $this->schemes[$i]['id'] . '/' . $dates[0]);
                            }
                            if (!$this->localContent->directoryExists($this->destDir . 'navsindex/' . $this->schemes[$i]['id'] . '/' . $dates[0] . '/' . $dates[1])) {
                                $this->localContent->createDirectory($this->destDir . 'navsindex/' . $this->schemes[$i]['id'] . '/' . $dates[0] . '/' . $dates[1]);
                            }

                            $this->localContent->write(
                                $this->destDir . 'navsindex/' . $this->schemes[$i]['id'] . '/' . $dates[0] . '/' . $dates[1] . '/' . $dates[2] . '.json', $this->helper->encode($dbNavNavs)
                            );
                        } catch (FilesystemException | UnableToCheckExistence | UnableToCreateDirectory | \throwable $e) {
                            $this->addResponse($e->getMessage(), 1);

                            return false;
                        }
                    }

                    $this->schemes[$i]['navs_last_updated'] = $dbNav['last_updated'];
                    $this->schemes[$i]['latest_nav'] = $this->helper->last($dbNav['navs'])['nav'];

                    try {
                        $this->localContent->write('.ff/sp/apps_fintech_mf_schemes/data/' . $this->schemes[$i]['id'] . '.json', $this->helper->encode($this->schemes[$i]));
                    } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                        $this->addResponse($e->getMessage(), 1);

                        return false;
                    }
                }

                $this->processUpdateTimer($dbCount, $i + 1);
            }
        } catch (\throwable $e) {
            trace([$e]);
            if (isset($data['scheme_id'])) {
                $schemeId = $data['scheme_id'];
            } else {
                $schemeId = $this->schemes[$i]['id'];
            }

            $this->basepackages->progress->setErrors([
                'error'     => 'Cannot process scheme nav for scheme id# ' . $schemeId,
                'message'   => $e->getMessage()
            ]);

            $this->addResponse('Cannot process scheme nav for scheme id# ' . $schemeId, 1, ['message' => $e->getMessage()]);

            return false;
        }

        return true;
    }

    protected function createChunks($dbNav, $data, $newNavs = false)
    {
        if (isset($data['get_all_navs']) && $data['get_all_navs'] == 'true') {
            $chunks = [];
            $chunks['id'] = (int) $dbNav['id'];
            $chunks['last_updated'] = $dbNav['last_updated'];
            $chunks['navs_chunks']['all'] = $dbNav['navs'];
        } else {
            try {
                if ($this->localContent->fileExists('.ff/sp/apps_fintech_mf_schemes_navs_chunks/data/' . $dbNav['id'] . '.json')) {
                    $chunks = $this->helper->decode($this->localContent->read('.ff/sp/apps_fintech_mf_schemes_navs_chunks/data/' . $dbNav['id'] . '.json'), true);

                    if (isset($chunks['navs_chunks']['all']) && count($chunks['navs_chunks']['all']) > 0) {
                        if ($this->helper->last($chunks['navs_chunks']['all'])['date'] === $this->helper->last($dbNav['navs'])['date']) {
                            return true;
                        }
                    }
                } else {
                    $chunks = [];
                    $chunks['id'] = (int) $dbNav['id'];
                    $chunks['last_updated'] = $dbNav['last_updated'];
                }
            } catch (FilesystemException | UnableToReadFile | UnableToCheckExistence | \throwable $e) {
                $this->addResponse($e->getMessage(), 1);

                return false;
            }

            if ($newNavs && count($newNavs) > 0) {
                if (isset($chunks['navs_chunks']['all']) && count($chunks['navs_chunks']['all']) > 0) {
                    $chunks['navs_chunks']['all'] = array_replace($chunks['navs_chunks']['all'], $newNavs);
                } else {
                    $chunks['navs_chunks']['all'] = $newNavs;
                }
            } else {
                if (isset($chunks['navs_chunks']['all']) && count($chunks['navs_chunks']['all']) > 0) {
                    $chunks['navs_chunks']['all'] = array_replace($chunks['navs_chunks']['all'], $dbNav['navs']);
                } else {
                    $chunks['navs_chunks']['all'] = $dbNav['navs'];
                }
            }
        }

        $datesKeys = array_keys($chunks['navs_chunks']['all']);

        foreach (['week', 'month', 'threeMonth', 'sixMonth', 'year', 'threeYear', 'fiveYear', 'tenYear'] as $time) {
            $latestDate = \Carbon\Carbon::parse($this->helper->lastKey($chunks['navs_chunks']['all']));
            $timeDate = null;

            if ($time === 'week') {
                $timeDate = $latestDate->subDay(6)->toDateString();
            } else if ($time === 'month') {
                $timeDate = $latestDate->subMonth()->toDateString();
            } else if ($time === 'threeMonth') {
                $timeDate = $latestDate->subMonth(3)->toDateString();
            } else if ($time === 'sixMonth') {
                $timeDate = $latestDate->subMonth(6)->toDateString();
            } else if ($time === 'year') {
                $timeDate = $latestDate->subYear()->toDateString();
            } else if ($time === 'threeYear') {
                $timeDate = $latestDate->subYear(3)->toDateString();
            } else if ($time === 'fiveYear') {
                $timeDate = $latestDate->subYear(5)->toDateString();
            } else if ($time === 'tenYear') {
                $timeDate = $latestDate->subYear(10)->toDateString();
            }

            if (isset($chunks['navs_chunks']['all'][$timeDate])) {
                $timeDateKey = array_search($timeDate, $datesKeys);
                $timeDateChunks = array_slice($chunks['navs_chunks']['all'], $timeDateKey);

                if (count($timeDateChunks) > 0) {
                    $chunks['navs_chunks'][$time] = [];

                    foreach ($timeDateChunks as $timeDateChunkDate => $timeDateChunk) {
                        $chunks['navs_chunks'][$time][$timeDateChunkDate] = [];
                        $chunks['navs_chunks'][$time][$timeDateChunkDate]['date'] = $timeDateChunk['date'];
                        $chunks['navs_chunks'][$time][$timeDateChunkDate]['nav'] = $timeDateChunk['nav'];
                        $chunks['navs_chunks'][$time][$timeDateChunkDate]['diff'] =
                            numberFormatPrecision($timeDateChunk['nav'] - $this->helper->first($timeDateChunks)['nav'], 4);
                        $chunks['navs_chunks'][$time][$timeDateChunkDate]['diff_percent'] =
                            numberFormatPrecision(($timeDateChunk['nav'] * 100 / $this->helper->first($timeDateChunks)['nav'] - 100), 2);
                    }
                }
            }
        }

        try {
            $this->localContent->write('.ff/sp/apps_fintech_mf_schemes_navs_chunks/data/' . $chunks['id'] . '.json', $this->helper->encode($chunks));
        } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }

        return true;
    }

    protected function createRollingReturns($dbNav, $i, $data, $newNavs = false)
    {
        if (isset($data['get_all_navs']) && $data['get_all_navs'] == 'true') {
            $rr = [];
            $rr['id'] = $dbNav['id'];
            $rr['last_updated'] = $dbNav['last_updated'];
        } else {
            try {
                if ($this->localContent->fileExists('.ff/sp/apps_fintech_mf_schemes_navs_rolling_returns/data/' . $dbNav['id'] . '.json')) {
                    $rr = $this->helper->decode($this->localContent->read('.ff/sp/apps_fintech_mf_schemes_navs_rolling_returns/data/' . $dbNav['id'] . '.json'), true);

                    if (isset($rr['year']) && count($rr['year']) > 0) {
                        if ($this->helper->last($rr['year'])['to'] === $this->helper->last($dbNav['navs'])['date']) {
                            return true;
                        }
                    }
                } else {
                    $rr = [];
                    $rr['id'] = $dbNav['id'];
                    $rr['last_updated'] = $dbNav['last_updated'];
                }
            } catch (FilesystemException | UnableToReadFile | UnableToCheckExistence | \throwable $e) {
                $this->addResponse($e->getMessage(), 1);

                return false;
            }
        }

        $schemeRr = [];
        $schemeRr['day_cagr'] = $this->helper->last($dbNav['navs'])['diff_percent'];
        $schemeRr['day_trajectory'] = $this->helper->last($dbNav['navs'])['trajectory'];
        $this->schemes[$i] = array_replace($this->schemes[$i], $schemeRr);
        try {
            $this->localContent->write('.ff/sp/apps_fintech_mf_schemes/data/' . $this->schemes[$i]['id'] . '.json', $this->helper->encode($this->schemes[$i]));
        } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }

        $latestDate = \Carbon\Carbon::parse($this->helper->lastKey($dbNav['navs']));
        $yearBefore = $latestDate->subYear()->toDateString();
        if (!isset($dbNav['navs'][$yearBefore])) {
            try {
                $this->localContent->write('.ff/sp/apps_fintech_mf_schemes_navs_rolling_returns/data/' . $rr['id'] . '.json', $this->helper->encode($rr));
            } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                $this->addResponse($e->getMessage(), 1);

                return false;
            }

            return true;
        }

        if (isset($data['get_all_navs']) && $data['get_all_navs'] == 'true') {
            $dbNavNavs = $dbNav['navs'];
        } else {
            if ($newNavs && count($newNavs) > 0) {
                $dbNavNavs = $newNavs;

                foreach ($dbNavNavs as $date => $nav) {
                    foreach (['year', 'two_year', 'three_year', 'five_year', 'seven_year', 'ten_year', 'fifteen_year'] as $rrTerm) {
                        try {
                            $toDate = \Carbon\Carbon::parse($date);

                            if ($rrTerm === 'year') {
                                $fromDate = $toDate->subYear()->toDateString();
                            } else if ($rrTerm === 'two_year') {
                                $fromDate = $toDate->subYear(2)->toDateString();
                            } else if ($rrTerm === 'three_year') {
                                $fromDate = $toDate->subYear(3)->toDateString();
                            } else if ($rrTerm === 'five_year') {
                                $fromDate = $toDate->subYear(5)->toDateString();
                            } else if ($rrTerm === 'seven_year') {
                                $fromDate = $toDate->subYear(7)->toDateString();
                            } else if ($rrTerm === 'ten_year') {
                                $fromDate = $toDate->subYear(10)->toDateString();
                            } else if ($rrTerm === 'fifteen_year') {
                                $fromDate = $toDate->subYear(15)->toDateString();
                            }

                            if (isset($dbNav['navs'][$fromDate])) {
                                $dbNavNavs[$fromDate] = $dbNav['navs'][$fromDate];
                            }
                        } catch (\throwable $e) {
                            $this->addResponse($e->getMessage(), 1);

                            return false;
                        }
                    }
                }

                $dbNavNavs = msort(array: $dbNavNavs, key: 'timestamp', preserveKey: true);
            } else {
                $dbNavNavs = $dbNav['navs'];
            }
        }

        $processingYear = null;
        $nationalHolidays = [];

        foreach ($dbNavNavs as $date => $nav) {
            foreach (['year', 'two_year', 'three_year', 'five_year', 'seven_year', 'ten_year', 'fifteen_year'] as $rrTerm) {
                try {
                    $fromDate = \Carbon\Carbon::parse($date);

                    if ($fromDate->isWeekend()) {
                        continue;
                    }

                    if (!$processingYear) {
                        $processingYear = $fromDate->year;
                    }

                    if ($processingYear !== $fromDate->year) {
                        $processingYear = $fromDate->year;

                        $this->getNationalHolidays($nationalHolidays, $processingYear);
                    } else {
                        if (!isset($nationalHolidays[$processingYear])) {
                            $this->getNationalHolidays($nationalHolidays, $processingYear);
                        }
                    }

                    if (in_array($date, $nationalHolidays[$processingYear])) {
                        continue;
                    }

                    $time = null;

                    if ($rrTerm === 'year') {
                        $toDate = $fromDate->addYear()->toDateString();
                        $time = 1;
                    } else if ($rrTerm === 'two_year') {
                        $toDate = $fromDate->addYear(2)->toDateString();
                        $time = 2;
                    } else if ($rrTerm === 'three_year') {
                        $toDate = $fromDate->addYear(3)->toDateString();
                        $time = 3;
                    } else if ($rrTerm === 'five_year') {
                        $toDate = $fromDate->addYear(5)->toDateString();
                        $time = 5;
                    } else if ($rrTerm === 'seven_year') {
                        $toDate = $fromDate->addYear(7)->toDateString();
                        $time = 7;
                    } else if ($rrTerm === 'ten_year') {
                        $toDate = $fromDate->addYear(10)->toDateString();
                        $time = 10;
                    } else if ($rrTerm === 'fifteen_year') {
                        $toDate = $fromDate->addYear(15)->toDateString();
                        $time = 15;
                    }

                    if (isset($rr[$rrTerm][$date])) {
                        continue;
                    }

                    if (isset($dbNavNavs[$toDate])) {
                        if (!isset($rr[$rrTerm])) {
                            $rr[$rrTerm] = [];
                        }

                        $rr[$rrTerm][$date]['from'] = $date;
                        $rr[$rrTerm][$date]['to'] = $toDate;
                        $rr[$rrTerm][$date]['cagr'] =
                            numberFormatPrecision((pow(($dbNavNavs[$toDate]['nav']/$nav['nav']),(1/$time)) - 1) * 100);

                        if ($toDate === $this->helper->last($dbNavNavs)['date']) {
                            $schemeRr[$rrTerm . '_cagr'] = $rr[$rrTerm][$date]['cagr'];
                        }
                    }
                } catch (\throwable $e) {
                    $this->addResponse($e->getMessage(), 1);

                    return false;
                }
            }
        }

        try {
            $this->localContent->write('.ff/sp/apps_fintech_mf_schemes_navs_rolling_returns/data/' . $rr['id'] . '.json', $this->helper->encode($rr));
        } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }

        //Calculate RR Average for timeframes. This will be used to narrow down our fund search.
        $rrCagrs = [];
        foreach ($rr as $rrTermType => $rrTermArr) {
            if (is_array($rrTermArr)) {
                foreach ($rrTermArr as $rrTermArrDate => $rrTermArrValue) {
                    if (!isset($rrCagrs[$rrTermType])) {
                        $rrCagrs[$rrTermType] = [];
                    }

                    $rrCagrs[$rrTermType][$rrTermArrDate] = $rrTermArrValue['cagr'];
                }
            }
        }
        if (count($rrCagrs) > 0) {
            foreach ($rrCagrs as $rrCagrTerm => $rrCagrArr) {
                $schemeRr[$rrCagrTerm . '_rr'] = numberFormatPrecision(\MathPHP\Statistics\Average::mean($rrCagrArr), 2);
            }
        }

        if (count($schemeRr) > 0) {
            $this->schemes[$i] = array_replace($this->schemes[$i], $schemeRr);

            try {
                $this->localContent->write('.ff/sp/apps_fintech_mf_schemes/data/' . $this->schemes[$i]['id'] . '.json', $this->helper->encode($this->schemes[$i]));
            } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                $this->addResponse($e->getMessage(), 1);

                return false;
            }
        }

        return true;
    }

    protected function getNationalHolidays(&$nationalHolidays, $processingYear)
    {
        $nationalHolidays[$processingYear] = [];

        $geoHolidays = $this->basepackages->geoHolidays->getNationalHolidays(null, $processingYear);

        if (count($geoHolidays) > 0) {
            foreach ($geoHolidays as $holiday) {
                if (!isset($this->parsedCarbon[$holiday['date']])) {
                    $this->parsedCarbon[$holiday['date']] = \Carbon\Carbon::parse($holiday['date']);
                }

                if ((\Carbon\Carbon::parse($holiday['date']))->isWeekend()) {
                    continue;
                }

                array_push($nationalHolidays[$processingYear], $holiday['date']);
            }
        }

        $commonPublicHolidays = ['01' => '26', '08' => '15', '10' => '02', '12' => '25'];//Holidays that repeat on the same day every year!
        array_walk($commonPublicHolidays, function($date, $month) use (&$nationalHolidays, $processingYear) {
            if (!in_array($processingYear . '-' . $month . '-' . $date, $nationalHolidays[$processingYear])) {
                array_push($nationalHolidays[$processingYear], $processingYear . '-' . $month . '-' . $date);
            }
        });
    }

    protected function fillAmfiNavDays($amfiNavsArr, $amfiCode)
    {
        // $firstDate = \Carbon\Carbon::parse($this->helper->first($amfiNavsArr)['date']);
        // $lastDate = \Carbon\Carbon::parse($this->helper->last($amfiNavsArr)['date']);

        // $numberOfDays = $firstDate->diffInDays($lastDate) + 1;//Include last day in calculation
        // var_dump($numberOfDays);
        $numberOfDays = (\Carbon\CarbonPeriod::between($this->helper->first($amfiNavsArr)['date'], $this->helper->last($amfiNavsArr)['date']))->toArray();
        // trace([count($numberOfDays)]);
        if (count($numberOfDays) != count($amfiNavsArr)) {
            $amfiNavs = [];

            foreach ($amfiNavsArr as $amfiNavKey => $amfiNav) {
                $amfiNavs[$amfiNav['date']] = $amfiNav;

                if (isset($amfiNavsArr[$amfiNavKey + 1])) {
                    $currentDate = \Carbon\Carbon::parse($amfiNav['date']);
                    if (!isset($this->parsedCarbon[$amfiNavsArr[$amfiNavKey + 1]['date']])) {
                        $this->parsedCarbon[$amfiNavsArr[$amfiNavKey + 1]['date']] = \Carbon\Carbon::parse($amfiNavsArr[$amfiNavKey + 1]['date']);
                    }
                    $nextDate = $this->parsedCarbon[$amfiNavsArr[$amfiNavKey + 1]['date']];
                    $differenceDays = $currentDate->diffInDays($nextDate);

                    if ($differenceDays > 1) {
                        for ($days = 1; $days < $differenceDays; $days++) {
                            $missingDay = $currentDate->addDay(1)->toDateString();

                            if (!isset($amfiNavs[$missingDay])) {
                                $amfiNav['date'] = $missingDay;

                                $amfiNavs[$amfiNav['date']] = $amfiNav;
                            }
                        }
                    }
                }
            }

            if (count($numberOfDays) != count($amfiNavs)) {
                throw new \Exception('Cannot process missing AMFI navs correctly for amfiCode : ' . $amfiCode);
            }

            return array_values($amfiNavs);
        }

        return $amfiNavsArr;
    }

    protected function reIndexMfSchemesData()
    {
        $this->method = 'reIndexMfSchemesData';

        $data = [];
        $data['task'] = 're-index';
        $data['selectedStores'] = ['apps_fintech_mf_schemes'];

        $reindex = $this->core->maintainFf($data);

        if (!$reindex) {
            $this->addResponse(
                $this->core->packagesData->responseMessage,
                $this->core->packagesData->responseCode
            );

            return false;
        }

        return true;
    }

    // protected function initDb($type, $data = [])
    // {
    //     try {
    //         $scanDir = $this->basepackages->utils->scanDir($this->destDir, false);

    //         if ($scanDir && count($scanDir['files']) > 0) {
    //             foreach ($scanDir['files'] as $file) {
    //                 if (str_ends_with($file, '-' . $type . '.db')) {
    //                     try {
    //                         return (new Sqlite())->init(base_path($file));
    //                     } catch (\throwable $e) {
    //                         $this->addResponse('Unable to open database file', 1);

    //                         return false;
    //                     }
    //                 }
    //             }
    //         }

    //         if (count($data) > 0) {
    //             $type === 'funds';
    //         }

    //         //File not exists, redownload
    //         if (!$this->localContent->fileExists($this->destDir . $this->today . '-' . $type . '.db')) {
    //             if (count($data) > 0) {
    //                 $this->addResponse('Download the latest funds file using Extractdata!', 1);

    //                 return false;
    //             }

    //             if ($type === 'latest') {
    //                 $this->downloadMfNavsData(true);
    //             } else {
    //                 $this->downloadMfNavsData(false, true);
    //             }

    //             $this->extractMfData();
    //         }
    //     } catch (FilesystemException | UnableToCheckExistence | \throwable $e) {
    //         $this->addResponse($e->getMessage(), 1);

    //         return false;
    //     }

    //     try {
    //         return (new Sqlite())->init(base_path($this->destDir . $this->today . '-' . $type . '.db'));
    //     } catch (\throwable $e) {
    //         $this->addResponse('Unable to open database file', 1);

    //         return false;
    //     }
    // }

    protected function processUpdateTimer($isinsTotal, $lineNo)
    {
        $this->basepackages->utils->setMicroTimer('End');

        $time = $this->basepackages->utils->getMicroTimer();

        if ($time && isset($time[1]['difference']) && $time[1]['difference'] !== 0) {
            $totalTime = date("H:i:s", floor($time[1]['difference'] * ($isinsTotal - $lineNo)));
        } else {
            $totalTime = date("H:i:s", 0);
        }

        $this->basepackages->utils->resetMicroTimer();

        if (PHP_SAPI !== 'cli') {
            $this->basepackages->progress->updateProgress(
                method: $this->method,
                counters: ['stepsTotal' => $isinsTotal, 'stepsCurrent' => ($lineNo + 1)],
                text: 'Time remaining : ' . $totalTime . '...'
            );
        }
    }

    public function sync($data)
    {
        if ($data['sync'] === 'gold') {
            return $this->processGold($data);
        }

        if ($data['sync'] === 'holidays') {
            return $this->processBankHolidays($data);
        }

        if (isset($data['api_id'])) {
            if (!$this->initApi($data)) {
                $this->addResponse('Could not initialize the API.', 1);

                return false;
            }
        }

        if (strtolower($this->apiClientConfig['provider']) === 'kuvera') {
            if (isset($data['sync']) && $data['sync'] === 'getSchemeDetails') {
                $kuveraMappings = $this->getKuveraMapping();

                if (isset($kuveraMappings[strtolower($data['isin'])])) {
                    $collection = 'MutualFundsApi';
                    $method = 'getFundSchemeDetails';
                    $args = [strtolower($kuveraMappings[strtolower($data['isin'])])];

                    $responseArr = $this->apiClient->useMethod($collection, $method, $args)->getResponse(true);

                    if ($responseArr && isset($responseArr[0])) {
                        return $responseArr[0];
                    }
                }
            }

            return [];
        }

        // Scheme Details
        // $method = 'getFundSchemeDetails';
        // $args = ['TRSSG1-GR'];

        //Gold Prices (Kuvera - prices are of 1G 22K + It seems like it a day old price Try not to use it.)
        // $collection = 'GoldApi';
        // $method = 'getCurrentGoldPrice';
        // $method = 'getGoldPrices';
        // $responseArr = $this->apiClient->useMethod($collection, $method, [])->getResponse(true);

        //Index Data Api
        // $collection = 'IndexDataApi';
        // $method = 'getNiftySmappcap100Value';
        // $responseArr = $this->apiClient->useMethod($collection, $method, [])->getResponse();

        //Rewrite:
        //All active schemes are to be downloaded from the AMFIIndia website:
        //https://portal.amfiindia.com/DownloadSchemeData_Po.aspx?mf=0
        //This will download a CSV file that needs to be parsed. File name: SchemeData2802250936SS (DateTimeSS)
        //Information available in the file:
        //AMC, Code, Scheme Name, Scheme Type, Scheme Category, Scheme NAV Name, Scheme Minimum Amount, Launch Date,  Closure Date, ISIN Div Payout/ ISIN GrowthISIN Div Reinvestment
        //With the above information, we can fill the AMCs database, Types database, Categories Database & Scheme Database (initial information of Name and ISIN)
        // $getArr = true;
        // if ($data['sync'] === 'schemeCategories') {
        //     $getArr = false;
        // }

        // $method = 'getFund' . ucfirst($data['sync']);

        // $responseArr = $this->apiClient->useMethod('MutualFundsApi', $method, [])->getResponse($getArr);

        // if ($responseArr && count($responseArr) > 0) {
        //     $process = 'process' . ucfirst($data['sync']);

        //     $this->$process($responseArr);

        //     return true;
        // }

        // $this->addResponse('Error processing sync', 1);

        return false;
    }

    protected function processAmcs(array $data)
    {
        if (isset($this->amcs[$data['AMC']])) {
            return $this->amcs[$data['AMC']];
        }

        if (!$this->amcsPackage) {
            $this->amcsPackage = new MfAmcs;
        }

        $amc = $this->amcsPackage->getMfAmcByName($data['AMC']);

        if (!$amc) {
            $amc = [];
            $amc['name'] = $data['AMC'];
            $amc['turn_around_time'] = null;

            $amc = $this->amcsPackage->addMfAmcs($amc);

            if ($amc) {
                $amc = [];
                $amc = $this->amcsPackage->packagesData->last;
            }
        }

        $this->amcs[$data['AMC']] = $amc;

        return $amc;
    }

    protected function processCategories(array $data)
    {
        if (!$this->categoriesPackage) {
            $this->categoriesPackage = new MfCategories;
        }

        $categories = explode('-', $data['Scheme Category']);

        if ($categories && (count($categories) === 1 || count($categories) === 2)) {
            array_walk($categories, function(&$category) {
                $category = trim($category);
            });

            if (count($categories) === 1) {
                if (isset($this->categories[$categories[0]])) {
                    return $this->categories[$categories[0]];
                }
            }

            $parentCategory = $this->categoriesPackage->getMfCategoryByName($categories[0]);

            if (!$parentCategory) {
                $parentCategory = [];
                $parentCategory['name'] = $categories[0];

                $this->categoriesPackage->addMfCategories($parentCategory);

                $parentCategory = $this->categoriesPackage->packagesData->last;
            }

            if (count($categories) === 2) {
                if (isset($this->categories[$categories[1]])) {
                    return $this->categories[$categories[1]];
                }

                $childCategory = $this->categoriesPackage->getMfCategoryByName($categories[1]);

                if (!$childCategory) {
                    $childCategory = [];
                    $childCategory['name'] = $categories[1];
                    $childCategory['parent_id'] = $parentCategory['id'];

                    $this->categoriesPackage->addMfCategories($childCategory);

                    $childCategory = $this->categoriesPackage->packagesData->last;
                }

                $this->categories[$categories[1]] = $childCategory;

                return $childCategory;
            }

            $this->categories[$categories[0]] = $parentCategory;

            return $parentCategory;
        }

        return false;
    }

    protected function processGold($data)
    {
        try {
            if ($this->localContent->fileExists($this->destDir . $this->today . '-gold' . '.json')) {
                $this->addResponse('File for ' . $this->today . ' already exists and imported.', 1);

                return false;
            }
        } catch (\throwable | UnableToCheckExistence $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }

        $days = 10;
        if (isset($data['days'])) {
            $days = $data['days'];

            if ($days > 249) {
                $days = 249;//249 days max, else you will get 500
            }

            if ($days === 0) {
                $days = 10;
            }
        }

        // Gold Prices via growww.in
        // https://groww.in/v1/api/physicalGold/v1/rates/aggregated_api?days=249
        // Days are number of days worth of data.
        $response = $this->remoteWebContent->get('https://groww.in/v1/api/physicalGold/v1/rates/aggregated_api?days=' . $days);

        if ($response->getStatusCode() === 200) {
            try {
                $this->localContent->write($this->destDir . $this->today . '-gold' . '.json', $response->getBody()->getContents());
            } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                $this->addResponse($e->getmessage(), 1);

                return false;
            }
        } else {
            $this->addResponse($response->getStatusCode() . ':' . $response->getMessage(), 1);

            return false;
        }

        //Perform Old files cleanup
       if (!$this->cleanup('gold')) {
            return false;
        }

        $this->addResponse('Imported Gold information successfully');

        return true;
    }

    protected function processBankHolidays($data)
    {
        //Regardless of where you get the data from, verification is required.
        if ($data['source'] === 'cleartax') {
            //https://cleartax.in/s/bank-holidays-list-{year}
            try {
                if (!$this->localContent->fileExists($this->destDir . $this->today . '-cleartax.html')) {
                    $this->cleanup('cleartax');

                    $response = $this->remoteWebContent->get('https://cleartax.in/s/bank-holidays-list-' . date('Y'));

                    if ($response->getStatusCode() === 200) {
                        try {
                            $this->localContent->write($this->destDir . $this->today . '-cleartax.html', $response->getBody()->getContents());
                        } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                            $this->addResponse($e->getmessage(), 1);

                            return false;
                        }
                    } else {
                        $this->addResponse($response->getStatusCode() . ':' . $response->getMessage(), 1);

                        return false;
                    }
                }

                $cleartaxHtml = $this->localContent->read($this->destDir . $this->today . '-cleartax.html');

                if ($cleartaxHtml !== '') {
                    $dbStatesArr = $this->basepackages->geoStates->searchStatesByCountryId('101');//India
                    $dbStates = [];

                    foreach ($dbStatesArr as $dbState) {
                        $firstWord = strtolower(explode(' ', $dbState['name'])[0]);

                        $dbStates[$firstWord] = $dbState['id'];
                    }

                    $stateMappings = [];

                    $holidays = [];

                    include('vendor/Simplehtmldom.php');

                    $html = file_get_html(base_path($this->destDir . $this->today . '-cleartax.html'));

                    $monthsTables = $html->find('table.ck-table-resized tbody');

                    foreach ($monthsTables as $table) {
                        foreach ($table->children as $key => $tr) {
                            $span = $tr->find('span');

                            if (count($span) < 3) {
                                continue;
                            }

                            $states = explode(',', $span[2]->plaintext);

                            if ($key === 0) {
                                if ($states[0] === 'states') {
                                    continue;
                                }
                            }

                            foreach ($states as $state) {
                                $stateFirstWord = strtolower(explode(' ', trim($state))[0]);

                                if ($stateFirstWord === 'chattisgarh') {
                                    $stateFirstWord = 'chhattisgarh';//spelling mistake observed in 2025 data.
                                }

                                if (isset($dbStates[$stateFirstWord])) {
                                    $stateMappings[$stateFirstWord] = $dbStates[$stateFirstWord];
                                }

                                try {
                                    $holidayDate = \Carbon\Carbon::createFromFormat('d F Y', explode(',', $span[0]->plaintext)[0]);

                                    if ($holidayDate->dayOfWeek === 7) {
                                        continue;
                                    }

                                    $holidayDate = $holidayDate->toDateString();
                                } catch (\throwable $e) {
                                    continue;
                                }

                                $holidayName = $span[1]->plaintext;

                                if (!isset($holidays[$holidayName])) {
                                    $holidays[$holidayName] = [];
                                }

                                if (!isset($holidays[$holidayName][$holidayDate])) {
                                    $holidays[$holidayName][$holidayDate] = [];
                                }

                                if (isset($dbStates[$stateFirstWord])) {
                                    array_push($holidays[$holidayName][$holidayDate], $dbStates[$stateFirstWord]);
                                }
                            }
                        }
                    }

                    foreach ($holidays as $holidayName => $holidayDates) {
                        foreach ($holidayDates as $holidayDate => $stateIds) {
                            $newHoliday = [];
                            $newHoliday['name'] = $holidayName;
                            $newHoliday['date'] = $holidayDate;
                            $newHoliday['country_id'] = '101';
                            $newHoliday['is_national_holiday'] = false;

                            if (count($stateMappings) === count($stateIds)) {
                                $newHoliday['is_national_holiday'] = true;
                                $newHoliday['state_id'] = 0;

                                try {
                                    $this->basepackages->geoHolidays->addHoliday($newHoliday);
                                } catch (\throwable $e) {
                                    continue;
                                }
                            } else {
                                foreach ($stateIds as $stateId) {
                                    try {
                                        $newHoliday['state_id'] = $stateId;

                                        $this->basepackages->geoHolidays->addHoliday($newHoliday);
                                    } catch (\throwable $e) {
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (\throwable | UnableToCheckExistence | UnableToWriteFile | UnableToReadFile $e) {
                $this->addResponse($e->getMessage(), 1);

                return false;
            }
        } else if ($data['source'] === 'groww') {
            //https://groww.in/banking/icici-bank-holidays
            try {
                if (!$this->localContent->fileExists($this->destDir . $this->today . '-groww.html')) {
                    $this->cleanup('groww');

                    $response = $this->remoteWebContent->get('https://groww.in/banking/icici-bank-holidays');

                    if ($response->getStatusCode() === 200) {
                        try {
                            $this->localContent->write($this->destDir . $this->today . '-groww.html', $response->getBody()->getContents());
                        } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                            $this->addResponse($e->getmessage(), 1);

                            return false;
                        }
                    } else {
                        $this->addResponse($response->getStatusCode() . ':' . $response->getMessage(), 1);

                        return false;
                    }
                }

                $growwHtml = $this->localContent->read($this->destDir . $this->today . '-groww.html');

                if ($growwHtml !== '') {
                    $dbStatesArr = $this->basepackages->geoStates->searchStatesByCountryId('101');//India
                    $dbStates = [];

                    foreach ($dbStatesArr as $dbState) {
                        $firstWord = strtolower(explode(' ', $dbState['name'])[0]);

                        $dbStates[$firstWord] = $dbState['id'];
                    }

                    $stateMappings = [];

                    $holidays = [];

                    include('vendor/Simplehtmldom.php');

                    $html = str_get_html($growwHtml);

                    $states = $html->find('h3.cs81Heading');

                    foreach ($states as $state) {
                        $stateFirstWord = strtolower(explode(' ', $state->plaintext)[0]);

                        if (isset($dbStates[$stateFirstWord])) {
                            $stateMappings[$stateFirstWord] = $dbStates[$stateFirstWord];

                            foreach ($state->parent()->children[1]->children[1]->children[0]->children as $key => $tr) {
                                if ($key === 0) {
                                    continue;
                                }

                                foreach ($tr->find('span') as $span => $spanValue) {
                                    if ($span === 0) {
                                        try {
                                            $holidayDate = \Carbon\Carbon::createFromFormat('d F Y', $spanValue->plaintext);

                                            if ($holidayDate->dayOfWeek === 7) {
                                                continue;
                                            }

                                            $holidayDate = $holidayDate->toDateString();
                                        } catch (\throwable $e) {
                                            continue;
                                        }
                                    }

                                    if ($span === 2) {
                                        $holidayName = $spanValue->plaintext;

                                        if (!isset($holidays[$holidayName])) {
                                            $holidays[$holidayName] = [];
                                        }

                                        if (!isset($holidays[$holidayName][$holidayDate])) {
                                            $holidays[$holidayName][$holidayDate] = [];
                                        }

                                        array_push($holidays[$holidayName][$holidayDate], $dbStates[$stateFirstWord]);
                                    }
                                }
                            }
                        }
                    }

                    foreach ($holidays as $holidayName => $holidayDates) {
                        foreach ($holidayDates as $holidayDate => $stateIds) {
                            $newHoliday = [];
                            $newHoliday['name'] = $holidayName;
                            $newHoliday['date'] = $holidayDate;
                            $newHoliday['country_id'] = '101';
                            $newHoliday['is_national_holiday'] = false;

                            if (count($stateMappings) === count($stateIds)) {
                                $newHoliday['is_national_holiday'] = true;
                                $newHoliday['state_id'] = 0;

                                try {
                                    $this->basepackages->geoHolidays->addHoliday($newHoliday);
                                } catch (\throwable $e) {
                                    continue;
                                }
                            } else {
                                foreach ($stateIds as $stateId) {
                                    try {
                                        $newHoliday['state_id'] = $stateId;

                                        $this->basepackages->geoHolidays->addHoliday($newHoliday);
                                    } catch (\throwable $e) {
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (\throwable | UnableToCheckExistence | UnableToWriteFile | UnableToReadFile $e) {
                $this->addResponse($e->getMessage(), 1);

                return false;
            }
        } else if ($data['source'] === 'timeandate') {
            //Time and date has correct holidays for national holidays, grab those!
            //https://www.timeanddate.com/holidays/india/2025?hol=1
        // }
        // } else if ($data['source'] === 'rbi') {
        //     //https://website.rbi.org.in/web/rbi/bank-holidays
        //     //Data is retrieved using Oauth token.
        //     //You can get the clientID and its secret via inspection of the webpage. Look for the JS code
        //     //https://website.rbi.org.in/o/oauth2/token
        //     //client_id=id-a499ea2d-8989-aac5-606e-75769097ea7
        //     //client_secret=secret-19ca301d-4289-3b31-361f-a6c626962c76
        //     //grant_type=client_credentials
        //     //Get States after authorization
        //     //https://website.rbi.org.in/o/rbi/bank-holidays/get-states-and-legends
        //     //

        //     try {
        //         if (!$this->localContent->fileExists($this->destDir . $this->today . '-rbitoken.json')) {
        //             $this->cleanup('rbi');

        //             $response = $this->remoteWebContent->request(
        //                 'POST',
        //                 'https://website.rbi.org.in/o/oauth2/token',
        //                 [
        //                     'form_params' =>
        //                         [
        //                             'client_id'         =>'id-a499ea2d-8989-aac5-606e-75769097ea7',
        //                             'client_secret'     =>'secret-19ca301d-4289-3b31-361f-a6c626962c76',
        //                             'grant_type'        =>'client_credentials'
        //                         ]
        //                 ]
        //             );

        //             if ($response->getStatusCode() === 200) {
        //                 try {
        //                     $this->localContent->write($this->destDir . $this->today . '-rbitoken.json', $response->getBody()->getContents());
        //                 } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
        //                     $this->addResponse($e->getmessage(), 1);

        //                     return false;
        //                 }
        //             } else {
        //                 $this->addResponse($response->getStatusCode() . ':' . $response->getMessage(), 1);

        //                 return false;
        //             }
        //         }

        //         $rbiJson = $this->helper->decode($this->localContent->read($this->destDir . $this->today . '-rbitoken.json'), true);
        //         // trace([$rbiJson]);

        //         if (!$this->localContent->fileExists($this->destDir . $this->today . '-rbistates.json')) {
        //             $response = $this->remoteWebContent->request(
        //                 'GET',
        //                 'https://website.rbi.org.in/o/rbi/bank-holidays/get-states-and-legends?languageCode=en',
        //                 [
        //                     'headers' =>
        //                         [
        //                             'Authorization' => 'Bearer ' . $rbiJson['access_token']
        //                         ]
        //                 ]
        //             );

        //             if ($response->getStatusCode() === 200) {
        //                 try {
        //                     $this->localContent->write($this->destDir . $this->today . '-rbistates.json', $response->getBody()->getContents());
        //                 } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
        //                     $this->addResponse($e->getmessage(), 1);

        //                     return false;
        //                 }
        //             } else {
        //                 $this->addResponse($response->getStatusCode() . ':' . $response->getMessage(), 1);

        //                 return false;
        //             }
        //         }

        //         $rbiStates = $this->helper->decode($this->localContent->read($this->destDir . $this->today . '-rbistates.json'), true);

        //         $dbStatesArr = $this->basepackages->geoStates->searchStatesByCountryId('101');//India
        //         $dbStates = [];

        //         foreach ($dbStatesArr as $dbState) {
        //             $firstWord = strtolower(explode(' ', $dbState['name'])[0]);

        //             $dbStates[$firstWord] = $dbState['id'];
        //         }

        //         $rbiStatesArr = [];

        //         $rbiStatesSorted = msort($rbiStates['listOfStates'], 'stateId');

        //         array_walk($rbiStatesSorted, function($state, $index,) use (&$rbiStatesArr, $dbStates) {
        //             $stateFirstWord = strtolower(explode(' ', $state['stateName'])[0]);

        //             if (isset($dbStates[$stateFirstWord])) {
        //                 $rbiStatesArr[$state['stateId']] = $stateFirstWord;
        //             }
        //         });

        //         $years = array_reverse($rbiStates['listOfYears']);

        //         //Send POSTRequest to https://website.rbi.org.in/o/rbi/bank-holidays/get-bank-holidays?languageCode=en
        //         //Request Data {"year":"2025","state":"21","legend":"","month":"all","viewType":"table"}
        //         foreach ($years as $year) {
        //             foreach ($rbiStatesArr as $rbiStateArrKey => $rbiStateArrFirstWord) {
        //                 $response = $this->remoteWebContent->request(
        //                     'POST',
        //                     'https://website.rbi.org.in/o/rbi/bank-holidays/get-bank-holidays?languageCode=en',
        //                     [
        //                         'headers' =>
        //                             [
        //                                 'Authorization' => 'Bearer ' . $rbiJson['access_token'],
        //                                 'Origin'        => 'https://website.rbi.org.in',
        //                                 'User-Agent'    => 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:139.0) Gecko/20100101 Firefox/139.0',
        //                                 'Accept'        => 'application/json, text/javascript, */*;',
        //                                 'Content-Type'  => 'application/json; charset=UTF-8',
        //                                 'Referer'       => 'https://website.rbi.org.in/web/rbi/bank-holidays',
        //                                 'Cookie'        => 'lanSelectorPopup-dontShowAgain=true-en; GUEST_LANGUAGE_ID=en_US; COOKIE_SUPPORT=true; _pk_id.6.394a=aab595357ac785c2.1749703629.; cookie-consent=true; cookie=!8xkP/UWXWoBUe7bnh65G0ZLrRPEVSmDuWLPTv1lOFmYCcVw2zdtkQh2es28uwGo5hZnMt79wrdNdGg==; lanSelectorPopup=true-en; mtm_consent=1749703629020; mtm_cookie_consent=1749703629020; JSESSIONID=4NSRWHFFh4moe--Noo5DR-PRyhnM9g4699W3zZGvOYYOcEcNCAyO!-1294097800; TS012a50ed=01af024724c85a94fa5d94e7ed29b14894caa63e46703afa7172a0c13f4b80246ef86fd5a7f359f7466982347dcf1fc6c24658f086e4724dbee2f4a358a61163cc752a3df4966a9fc933a24c44f20343f3f41382a6a2be767191f1a1336ac466632a7906bd421f7934990a9a110dbb37c8e4a19881; LFR_SESSION_STATE_20103=1750507539537; ASLBSA=000358fe6bf303f1c694cc8d4777131ee6a2e67c37d641dc89afe795ce29ee532352; ASLBSACORS=000358fe6bf303f1c694cc8d4777131ee6a2e67c37d641dc89afe795ce29ee532352; _pk_ses.6.394a=1'

        //                             ],
        //                         'form_params' =>
        //                             [
        //                                 'year'          => $year,
        //                                 'state'         => $rbiStateArrKey,
        //                                 'legend'        => '',
        //                                 'month'         => 'all',
        //                                 'viewType'      => 'table'
        //                             ]
        //                     ]
        //                 );
        //                 trace([$response]);

        //                 if ($response->getStatusCode() === 200) {
        //                     trace([$response->getBody()->getContents()]);
        //                     try {
        //                         $this->localContent->write($this->destDir . $this->today . '-rbistates.json', $response->getBody()->getContents());
        //                     } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
        //                         $this->addResponse($e->getmessage(), 1);

        //                         return false;
        //                     }
        //                 } else {
        //                     $this->addResponse($response->getStatusCode() . ':' . $response->getMessage(), 1);

        //                     return false;
        //                 }

        //             }
        //             trace([$year]);
        //         }
        //         if ($rbiJson !== '') {
        //             $stateMappings = [];

        //             $holidays = [];

        //             include('vendor/Simplehtmldom.php');

        //             $html = str_get_html($rbiJson);

        //             $states = $html->find('select#stateListSelect');
        //             trace([count($states[0]->children)]);
        //             foreach ($states[0]->children as $state) {
        //                 dump($state);
        //                 die();
        //                 $attributes = $state->getAllAttributes();
        //                 // if (!in_array('data-state-code', $attributes)) {
        //                 //     continue;
        //                 // }

        //                 // $stateFirstWord = strtolower(explode(' ', $state->plaintext)[0]);

        //                 // if (isset($dbStates[$stateFirstWord])) {
        //                 //     $stateMappings[$stateFirstWord] = $dbStates[$stateFirstWord];

        //                 //     foreach ($state->parent()->children[1]->children[1]->children[0]->children as $key => $tr) {
        //                 //         if ($key === 0) {
        //                 //             continue;
        //                 //         }

        //                 //         foreach ($tr->find('span') as $span => $spanValue) {
        //                 //             if ($span === 0) {
        //                 //                 try {
        //                 //                     $holidayDate = \Carbon\Carbon::createFromFormat('d F Y', $spanValue->plaintext);

        //                 //                     if ($holidayDate->dayOfWeek === 7) {
        //                 //                         continue;
        //                 //                     }

        //                 //                     $holidayDate = $holidayDate->toDateString();
        //                 //                 } catch (\throwable $e) {
        //                 //                     continue;
        //                 //                 }
        //                 //             }

        //                 //             if ($span === 2) {
        //                 //                 $holidayName = $spanValue->plaintext;

        //                 //                 if (!isset($holidays[$holidayName])) {
        //                 //                     $holidays[$holidayName] = [];
        //                 //                 }

        //                 //                 if (!isset($holidays[$holidayName][$holidayDate])) {
        //                 //                     $holidays[$holidayName][$holidayDate] = [];
        //                 //                 }

        //                 //                 array_push($holidays[$holidayName][$holidayDate], $dbStates[$stateFirstWord]);
        //                 //             }
        //                 //         }
        //                 //     }
        //                 // }
        //             }
        //             die();
        //             foreach ($holidays as $holidayName => $holidayDates) {
        //                 foreach ($holidayDates as $holidayDate => $stateIds) {
        //                     $newHoliday = [];
        //                     $newHoliday['name'] = $holidayName;
        //                     $newHoliday['date'] = $holidayDate;
        //                     $newHoliday['is_national_holiday'] = false;

        //                     if (count($stateMappings) === count($stateIds)) {
        //                         $newHoliday['is_national_holiday'] = true;
        //                         $newHoliday['state_id'] = 0;

        //                         try {
        //                             $this->basepackages->geoHolidays->addHoliday($newHoliday);
        //                         } catch (\throwable $e) {
        //                             continue;
        //                         }
        //                     } else {
        //                         foreach ($stateIds as $stateId) {
        //                             try {
        //                                 $newHoliday['state_id'] = $stateId;

        //                                 $this->basepackages->geoHolidays->addHoliday($newHoliday);
        //                             } catch (\throwable $e) {
        //                                 continue;
        //                             }
        //                         }
        //                     }
        //                 }
        //             }
        //         }
        //     } catch (\throwable | UnableToCheckExistence | UnableToWriteFile | UnableToReadFile $e) {
        //         trace([$e]);
        //         $this->addResponse($e->getMessage(), 1);

        //         return false;
        //     }
        }

        $this->addResponse('Imported holiday information via ' . $data['source'] . ' successfully');

        return true;
    }

    protected function initApi($data, $sink = null, $method = null)
    {
        if ($this->apiClient && $this->apiClientConfig) {
            return true;
        }

        if (!isset($data['api_id'])) {
            $this->addResponse('API information not provided', 1, []);

            return false;
        }

        if (isset($data['api_id']) && $data['api_id'] == '0') {
            $this->addResponse('This is local module and not remote module, cannot sync.', 1, []);

            return false;
        }

        if ($sink & $method) {
            $this->apiClient = $this->basepackages->apiClientServices->setHttpOptions(['timeout' => 3600])->setMonitorProgress($sink, $method)->useApi($data['api_id']);
        } else {
            $this->apiClient = $this->basepackages->apiClientServices->useApi($data['api_id']);
        }

        $this->apiClientConfig = $this->apiClient->getApiConfig();

        if ($this->apiClientConfig['auth_type'] === 'auth' &&
            ((!$this->apiClientConfig['username'] || $this->apiClientConfig['username'] === '') &&
            (!$this->apiClientConfig['password'] || $this->apiClientConfig['password'] === ''))
        ) {
            $this->addResponse('Username/Password missing, cannot sync', 1);

            return false;
        } else if ($this->apiClientConfig['auth_type'] === 'access_token' &&
                  (!$this->apiClientConfig['access_token'] || $this->apiClientConfig['access_token'] === '')
        ) {
            $this->addResponse('Access token missing, cannot sync', 1);

            return false;
        } else if ($this->apiClientConfig['auth_type'] === 'autho' &&
                  (!$this->apiClientConfig['authorization'] || $this->apiClientConfig['authorization'] === '')
        ) {
            $this->addResponse('Authorization token missing, cannot sync', 1);

            return false;
        }

        return true;
    }

    public function getAvailableApis($getAll = false, $returnApis = true)
    {
        $apisArr = [];

        if (!$getAll) {
            $package = $this->getPackage();
            if (isset($package['settings']) &&
                isset($package['settings']['api_clients']) &&
                is_array($package['settings']['api_clients']) &&
                count($package['settings']['api_clients']) > 0
            ) {
                foreach ($package['settings']['api_clients'] as $key => $clientId) {
                    $client = $this->basepackages->apiClientServices->getApiById($clientId);

                    if ($client) {
                        array_push($apisArr, $client);
                    }
                }
            }
        } else {
            $apisArr = $this->basepackages->apiClientServices->getApiByAppType();
            $apisArr = array_merge($apisArr, $this->basepackages->apiClientServices->getApiByAppType('core'));
        }

        if (count($apisArr) > 0) {
            foreach ($apisArr as $apisArrKey => $api) {
                if ($api['category'] === 'repos' || $api['category'] === 'providers') {
                    $useApi = $this->basepackages->apiClientServices->useApi([
                            'config' =>
                                [
                                    'id'           => $api['id'],
                                    'category'     => $api['category'],
                                    'provider'     => $api['provider'],
                                    'checkOnly'    => true//Set this to check if the API exists and can be instantiated.
                                ]
                        ]);

                    if ($useApi) {
                        $apiConfig = $useApi->getApiConfig();

                        if (isset($apiConfig['repo_url']) && !str_contains($apiConfig['repo_url'], 'sp-fintech-mutualfunds')) {
                            unset($apisArr[$apisArrKey]);

                            continue;
                        }

                        $apis[$api['id']]['id'] = $apiConfig['id'];
                        $apis[$api['id']]['name'] = $apiConfig['name'];
                        if (isset($apiConfig['repo_url'])) {
                            $apis[$api['id']]['data']['url'] = $apiConfig['repo_url'];
                        } else if (isset($apiConfig['api_url'])) {
                            $apis[$api['id']]['data']['url'] = $apiConfig['api_url'];
                        }
                    }
                }
            }
        }

        if ($returnApis) {
            return $apis ?? [];
        }

        return $apisArr;
    }

    protected function getKuveraMapping()
    {
        $this->sourceLink = 'https://raw.githubusercontent.com/captn3m0/india-mutual-funds-info/refs/heads/main/data.csv';

        $this->destFile = base_path($this->destDir) . $this->today . '-kuvera.csv';

        try {
            $download = false;
            //File is already downloaded
            if ($this->localContent->fileExists($this->destDir . $this->today . '-kuvera.csv')) {
                $remoteSize = (int) getRemoteFilesize($this->sourceLink);

                $localSize = $this->localContent->fileSize($this->destDir . $this->today . '-kuvera.csv');

                if ($remoteSize !== $localSize) {
                    $this->downloadData($this->sourceLink, $this->destFile);

                    $download = true;
                }
            } else {
                $this->downloadData($this->sourceLink, $this->destFile);

                $download = true;
            }

            $mappings = [];

            if ($this->opCache) {
                $mappings = $this->opCache->getCache('mappings', 'mfmappings');
            } else {
                $mappings = $this->helper->decode($this->localContent->read('/var/mfmappings/mappings.json'), true);
            }

            if (!$mappings ||
                ($mappings && count($mappings) === 0)
            ) {
                if (!$mappings) {
                    $mappings = [];
                }

                $csv = Reader::createFromStream($this->localContent->readStream($this->destDir . $this->today . '-kuvera.csv'));
                $csv->setHeaderOffset(0);

                $records = $csv->getRecords();

                foreach ($records as $line) {
                    if (!isset($mappings[strtolower($line['ISIN'])])) {
                        $mappings[strtolower($line['ISIN'])] = strtolower($line['code']);
                    }
                }

                if ($this->opCache) {
                    $this->opCache->setCache('mappings', $mappings, 'mfmappings');
                } else {
                    $this->localContent->write('var/mfmappings/mappings.json' , $this->helper->encode($mappings));
                }
            }

            return $mappings;
        } catch (FilesystemException | UnableToCheckExistence | UnableToRetrieveMetadata | UnableToReadFile | UnableToWriteFile | \throwable $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }
    }
}
<?php

namespace Apps\Fintech\Packages\Mf\Extractdata;

use Apps\Fintech\Packages\Mf\Amcs\MfAmcs;
use Apps\Fintech\Packages\Mf\Categories\MfCategories;
use Apps\Fintech\Packages\Mf\Extractdata\Settings;
use Apps\Fintech\Packages\Mf\Navs\MfNavs;
use Apps\Fintech\Packages\Mf\Schemes\MfSchemes;
use Apps\Fintech\Packages\Mf\Types\MfTypes;
use League\Csv\Reader;
use League\Csv\Statement;
use League\Flysystem\FilesystemException;
use League\Flysystem\UnableToCheckExistence;
use League\Flysystem\UnableToDeleteFile;
use League\Flysystem\UnableToWriteFile;
use Phalcon\Db\Enum;
use System\Base\BasePackage;
use System\Base\Providers\DatabaseServiceProvider\Sqlite;

class MfExtractdata extends BasePackage
{
    protected $now;

    protected $today;

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

    protected $processAll = false;

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

    protected function downloadMfNavsData($downloadLatestNav = false, $downloadAllNav = true)
    {
        $this->method = 'downloadMfNavsData';

        if ($downloadLatestNav) {
            try {
                if (!$this->localContent->fileExists($this->destDir . $this->previousDay . '-latest.db.zst')) {
                    $this->downloadMfNavsData(false, true);

                    $this->processAll = true;
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

            $this->destFile = base_path($this->destDir) . $this->today . '-funds.db.zst';

            try {
                //File is already downloaded
                if ($this->localContent->fileExists($this->destDir . $this->today . '-funds.db.zst')) {
                    $remoteSize = (int) getRemoteFilesize($this->sourceLink);

                    $localSize = $this->localContent->fileSize($this->destDir . $this->today . '-funds.db.zst');

                    if ($remoteSize === $localSize) {
                        $this->mfFileSizeMatch['funds'] = true;

                        return true;
                    }
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
                    if (!str_starts_with($file, $this->today) &&
                        str_contains($file, $type)
                    ) {
                        $this->localContent->delete($file);
                    }
                }
            }
        } catch (UnableToDeleteFile | \throwable | FilesystemException $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }

        return true;
    }

    protected function extractMfSchemesData($extractLatestNav = false, $extractAllNav = true)
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

    protected function extractMfNavsData($extractLatestNav = false, $extractAllNav = true)
    {
        $this->method = 'extractMfNavsData';

        $files = [];

        if ($extractLatestNav) {
            if ($this->localContent->fileExists($this->destDir . $this->today . '-latest.db.zst')) {
                array_push($files, '-latest');
            }
        }

        if ($extractAllNav) {
            if ($this->localContent->fileExists($this->destDir . $this->today . '-funds.db.zst')) {
                array_push($files, '-funds');
            }
        }

        if (count($files) === 0) {
            $this->addResponse('Nothing to extract!', 1);

            return false;
        }

        foreach ($files as $file) {
            try {
                if ($this->localContent->fileExists($this->destDir . $this->today . $file . '.db')) {
                    if (str_contains($file, '-latest')) {
                        if (isset($this->mfFileSizeMatch) &&
                            $this->mfFileSizeMatch['latest'] === true
                        ) {//If compressed file match, the decompressed and indexed will also match.
                            continue;
                        }
                    } else if (str_contains($file, '-funds')) {
                        if (isset($this->mfFileSizeMatch) &&
                            $this->mfFileSizeMatch['funds'] === true
                        ) {//If compressed file match, the decompressed and indexed will also match.
                            continue;
                        }
                    }

                    $this->localContent->delete($this->destDir . $this->today . $file . '.db');
                }

                //Decompress
                exec('unzstd -d -f ' . base_path($this->destDir) . $this->today . $file . '.db.zst -o ' . base_path($this->destDir) . $this->today . $file . '.db', $output, $result);
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
                exec("echo 'CREATE INDEX \"nav-main\" ON \"nav\" (\"date\",\"scheme_code\")' | sqlite3 " . base_path($this->destDir) . $this->today . $file . ".db", $output, $result);
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

                exec("echo 'CREATE INDEX \"nav-scheme\" ON \"nav\" (\"scheme_code\")' | sqlite3 " . base_path($this->destDir) . $this->today . $file . ".db", $output, $result);
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

                exec("echo 'CREATE INDEX \"securities-scheme\" ON \"securities\" (\"scheme_code\")' | sqlite3 " . base_path($this->destDir) . $this->today . $file . ".db", $output, $result);
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

                exec("echo 'CREATE INDEX \"securities-isin\" ON \"securities\" (\"isin\")' | sqlite3 " . base_path($this->destDir) . $this->today . $file . ".db", $output, $result);
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
            $this->schemesPackage = new MfSchemes;

            $csv = Reader::createFromStream($this->localContent->readStream($this->destDir . $this->today . '-schemes.csv'));
            $csv->setHeaderOffset(0);

            $statement = (new Statement())->orderByAsc('AMC');
            $records = $statement->process($csv);

            $isinsTotal = count($records);
            $lineNo = 1;

            foreach ($records as $line) {
                //Timer
                $this->basepackages->utils->setMicroTimer('Start');

                $isinArr = explode('INF', $line['ISIN Div Payout/ ISIN GrowthISIN Div Reinvestment']);

                if (strlen($line['ISIN Div Payout/ ISIN GrowthISIN Div Reinvestment']) === 0) {
                    $isinArr[0] = '';
                    $isinArr[1] = 'INF_' . hash('md5', $line['Code']);
                }

                if (count($isinArr) === 1 &&
                    ($isinArr[0] === '' ||
                     $isinArr[0] === 'xxxxxxxxxxxxxxxxxxx' ||
                     $isinArr[0] === $line['ISIN Div Payout/ ISIN GrowthISIN Div Reinvestment'])
                ) {
                    $isinArr[1] = 'INF_' . hash('md5', $line['Code']);
                }

                $scheme = $this->schemesPackage->getMfTypeByAmfiCode($line['Code']);

                if ($scheme) {
                    if ($scheme['amfi_code'] == $line['Code']) {
                        $this->processUpdateTimer($isinsTotal, $lineNo);

                        $lineNo++;

                        continue;
                    } else {
                        //We found a duplicate entry in the CSV file with different amfi_code
                        if (count($isinArr) === 2) {
                            $isinArr[1] = 'INF' . trim($isinArr[1]) . '_duplicate';
                        } else if (count($isinArr) === 3) {
                            $isinArr[1] = 'INF' . trim($isinArr[1]) . '_duplicate';
                            $isinArr[2] = 'INF' . trim($isinArr[2]) . '_duplicate';
                        }
                    }
                }

                $scheme = [];

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
                $scheme['amfi_code'] = $line['Code'];
                $scheme['scheme_type'] = $line['Scheme Type'];
                $scheme['category_id'] = $category['id'];
                $scheme['name'] = $line['Scheme NAV Name'];
                if (str_contains(strtolower($line['Scheme NAV Name']), 'direct')) {
                    $scheme['expense_ratio_type'] = 'Direct';
                } else if (str_contains(strtolower($line['Scheme NAV Name']), 'regular')) {
                    $scheme['expense_ratio_type'] = 'Regular';
                } else {
                    $scheme['expense_ratio_type'] = 'Direct';
                }
                if (str_contains(strtolower($line['Scheme NAV Name']), 'growth')) {
                    $scheme['plan_type'] = 'Growth';
                } else if (str_contains(strtolower($line['Scheme NAV Name']), 'idcw') ||
                           str_contains(strtolower($line['Scheme NAV Name']), 'dividend') ||
                           str_contains(strtolower($line['Scheme NAV Name']), 'income distribution')
                ) {
                    $scheme['plan_type'] = 'IDCW';
                } else {
                    $scheme['plan_type'] = 'Growth';
                }
                if (str_contains(strtolower($line['Scheme NAV Name']), 'passive')) {
                    $scheme['management_type'] = 'Passive';
                } else {
                    $scheme['management_type'] = 'Active';
                }

                if (isset($scheme['id'])) {
                    $this->schemesPackage->update($scheme);
                } else {
                    $this->schemesPackage->add($scheme);
                }

                //Timer
                $this->processUpdateTimer($isinsTotal, $lineNo);

                $lineNo++;
            }
        } catch (\throwable $e) {
            $this->addResponse($e->getMessage(), 1, ['line' => $this->helper->encode($line)]);

            $errors['exception'] = $e->getMessage();
            $errors['lineNo'] = $lineNo;
            $errors['line'] = $this->helper->encode($line);

            $this->basepackages->progress->setErrors($errors);

            $this->basepackages->progress->resetProgress();

            throw $e;
        }

        return true;
    }

    protected function processMfNavsData($processLatestNav = false, $processAllNav = true, $data = [])
    {
        $this->method = 'processMfNavsData';

        if ($this->processAll) {
            $processLatestNav = false;
            $processAllNav = true;
        }

        if ($processLatestNav && count($data) === 0) {
            if (!$sqlite = $this->initDb('latest', $data)) {
                return false;
            }

            $this->navsPackage = new MfNavs;
            $this->schemesPackage = new MfSchemes;

            $dbCount = $this->schemesPackage->getDbCount(true);

            if ($dbCount > 0) {
                for ($i = 1; $i <= $dbCount; $i++) {
                    $this->basepackages->utils->setMicroTimer('Start');

                    if (isset($data['scheme_id'])) {
                        $scheme = $this->schemesPackage->getById((int) $data['scheme_id']);
                    } else {
                        $scheme = $this->schemesPackage->getById($i);
                    }

                    if ($scheme) {
                        $amfiCode = $scheme['amfi_code'];

                        $amfiNavs = $sqlite->query(
                            "SELECT * from nav N
                            JOIN securities S ON N.scheme_code = S.scheme_code
                            WHERE S.scheme_code = '" . $amfiCode . "' AND S.type = '0'
                            ORDER BY N.date ASC"
                        )->fetchAll(Enum::FETCH_ASSOC);

                        if (!$amfiNavs) {
                            $this->processUpdateTimer($dbCount, $i);

                            continue;
                        }

                        $amfiNavs = $this->fillAmfiNavDays($amfiNavs);

                        $dbNav = $this->navsPackage->getMfNavsByAmfiCode($amfiCode);

                        if (!$dbNav) {
                            $dbNav = [];
                            $dbNav['amfi_code'] = $scheme['amfi_code'];
                            $dbNav['navs'] = [];
                        }

                        if ($amfiNavs && count($amfiNavs) === 1) {
                            if ($amfiNavs[0]['date']) {
                                $dbNav['last_updated'] = $amfiNavs[0]['date'];
                            } else {
                                $dbNav['last_updated'] = $this->today;
                            }

                            if (isset($amfiNavs[0]['nav'])) {
                                $dbNav['latest_nav'] = $amfiNavs[0]['nav'];
                            } else {
                                $dbNav['latest_nav'] = 0;
                            }

                            if (!isset($dbNav['navs'][$amfiNavs[0]['date']])) {
                                $date = \Carbon\Carbon::parse($amfiNavs[0]['date']);
                                $dbNav['navs'][$amfiNavs[0]['date']]['nav'] = $amfiNavs[0]['nav'];
                                $dbNav['navs'][$amfiNavs[0]['date']]['date'] = $amfiNavs[0]['date'];
                                $dbNav['navs'][$amfiNavs[0]['date']]['timestamp'] = $date->timestamp;
                                $previousDay = $date->subDay(1)->toDateString();

                                if (isset($dbNav['navs'][$previousDay])) {
                                    $dbNav['navs'][$amfiNavs[0]['date']]['diff'] =
                                        numberFormatPrecision($amfiNavs[0]['nav'] - $dbNav['navs'][$previousDay]['nav'], 4);
                                    $dbNav['navs'][$amfiNavs[0]['date']]['diff_percent'] =
                                        numberFormatPrecision(($amfiNavs[0]['nav'] * 100 / $dbNav['navs'][$previousDay]['nav']) - 100, 2);

                                    $dbNav['navs'][$amfiNavs[0]['date']]['trajectory'] = '-';
                                    if ($amfiNavs[0]['nav'] > $dbNav['navs'][$previousDay]['nav']) {
                                        $dbNav['navs'][$amfiNavs[0]['date']]['trajectory'] = 'up';
                                    } else {
                                        $dbNav['navs'][$amfiNavs[0]['date']]['trajectory'] = 'down';
                                    }

                                    $dbNav['navs'][$amfiNavs[0]['date']]['diff_since_inception'] =
                                        numberFormatPrecision($amfiNavs[0]['nav'] - $this->helper->first($dbNav['navs'])['nav'], 4);
                                    $dbNav['navs'][$amfiNavs[0]['date']]['diff_percent_since_inception'] =
                                        numberFormatPrecision(($amfiNavs[0]['nav'] * 100 / $this->helper->first($dbNav['navs'])['nav'] - 100), 2);
                                }
                            }

                            $this->createChunks($dbNav);
                        } else {
                            $dbNav['last_updated'] = $this->today;
                            $dbNav['latest_nav'] = 0;
                        }

                        if (isset($dbNav['id'])) {
                            $this->navsPackage->update($dbNav);
                        } else {
                            $this->navsPackage->add($dbNav);
                        }

                        $this->processUpdateTimer($dbCount, $i);
                    }
                }
            }
        }

        if ($processAllNav) {
            if (!$sqlite = $this->initDb('funds', $data)) {
                return false;
            }

            $this->navsPackage = $this->usePackage(MfNavs::class);
            $this->schemesPackage = $this->usePackage(MfSchemes::class);

            $lastUpdated = $this->weekAgo;

            if (isset($data['get_all_navs']) && $data['get_all_navs'] == 'true') {
                $lastUpdated = '2000-01-01';
            }

            //Subtract year on every Sunday
            if (!isset($data['get_all_navs']) && $this->now->dayOfWeek === 0) {
                $lastUpdated = $this->now->subYear()->toDateString();
            }

            if (isset($data['scheme_id'])) {
                $dbCount = 1;
            } else {
                $dbCount = $this->schemesPackage->getLastInsertedId();
            }

            if ($dbCount > 0) {
                for ($i = 1; $i <= $dbCount; $i++) {
                    $this->basepackages->utils->setMicroTimer('Start');

                    if (isset($data['scheme_id'])) {
                        $scheme = $this->schemesPackage->getById((int) $data['scheme_id']);
                    } else {
                        $scheme = $this->schemesPackage->getById($i);
                    }

                    if ($scheme) {
                        $amfiCode = $scheme['amfi_code'];

                        $amfiNavs = $sqlite->query(
                            "SELECT * from nav N
                            JOIN securities S ON N.scheme_code = S.scheme_code
                            WHERE S.scheme_code = '" . $amfiCode . "' AND S.type = '0'
                            AND N.date >= '$lastUpdated'
                            ORDER BY N.date ASC"
                        )->fetchAll(Enum::FETCH_ASSOC);

                        if (!$amfiNavs) {
                            $this->processUpdateTimer($dbCount, $i);

                            continue;
                        }

                        $amfiNavs = $this->fillAmfiNavDays($amfiNavs);

                        $dbNav = $this->navsPackage->getMfNavsByAmfiCode($amfiCode);

                        if (!$dbNav) {
                            $dbNav = [];
                            $dbNav['amfi_code'] = $scheme['amfi_code'];
                            $dbNav['navs'] = [];
                        } else {
                            $lastUpdated = $dbNav['last_updated'];

                            if (isset($data['get_all_navs']) && $data['get_all_navs'] == 'true') {
                                if (!isset($dbNav['navs'])) {
                                    $dbNav['navs'] = [];
                                }
                            }
                        }

                        if ($amfiNavs && count($amfiNavs) > 0) {
                            if (!isset($data['get_all_navs']) &&
                                isset($dbNav['last_updated']) &&
                                $this->helper->last($amfiNavs)['date'] === $dbNav['last_updated']
                            ) {
                                $this->processUpdateTimer($dbCount, $i);

                                continue;
                            }

                            if ($this->helper->last($amfiNavs)['date']) {
                                $dbNav['last_updated'] = $this->helper->last($amfiNavs)['date'];
                            } else {
                                $dbNav['last_updated'] = $this->today;
                            }

                            if ($this->helper->last($amfiNavs)['nav']) {
                                $dbNav['latest_nav'] = $this->helper->last($amfiNavs)['nav'];
                            } else {
                                $dbNav['latest_nav'] = 0;
                            }

                            $newdata = false;

                            foreach ($amfiNavs as $amfiNavKey => $amfiNav) {
                                if (!isset($dbNav['navs'][$amfiNav['date']])) {
                                    $newdata = true;
                                    $dbNav['navs'][$amfiNav['date']]['nav'] = $amfiNav['nav'];
                                    $dbNav['navs'][$amfiNav['date']]['date'] = $amfiNav['date'];
                                    $dbNav['navs'][$amfiNav['date']]['timestamp'] = \Carbon\Carbon::parse($amfiNav['date'])->timestamp;

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

                                        if ($amfiNavKey === $this->helper->lastKey($amfiNavs)) {
                                            $dbNav['diff'] = $dbNav['navs'][$amfiNav['date']]['diff'];
                                            $dbNav['diff_percent'] = $dbNav['navs'][$amfiNav['date']]['diff_percent'];
                                            $dbNav['trajectory'] = $dbNav['navs'][$amfiNav['date']]['trajectory'];
                                        }

                                        if ($amfiNavKey !== 0) {
                                            $dbNav['navs'][$amfiNav['date']]['diff_since_inception'] =
                                                numberFormatPrecision($amfiNav['nav'] - $amfiNavs[0]['nav'], 4);
                                            $dbNav['navs'][$amfiNav['date']]['diff_percent_since_inception'] =
                                                numberFormatPrecision(($amfiNav['nav'] * 100 / $amfiNavs[0]['nav'] - 100), 2);
                                        }
                                    }
                                }
                            }

                            if (!$newdata) {
                                $this->processUpdateTimer($dbCount, $i);

                                continue;
                            }

                            $this->createChunks($dbNav);
                        } else {
                            $dbNav['last_updated'] = $this->today;
                            $dbNav['latest_nav'] = 0;
                        }

                        if (isset($dbNav['id'])) {
                            $dbNav['navs'] = msort(array: $dbNav['navs'], key: 'timestamp', preserveKey: true);

                            $this->navsPackage->update($dbNav);
                        } else {
                            $this->navsPackage->add($dbNav);
                        }

                        $this->processUpdateTimer($dbCount, $i);
                    }
                }
            }
        }

        return true;
    }

    protected function createChunks(&$dbNav)
    {
        $dbNavNavs = msort($dbNav['navs'], 'timestamp');

        $totalNavs = count($dbNavNavs);

        if ($totalNavs > 1) {
            if ($totalNavs > 7) {
                $forWeek = $totalNavs - 7;
                $forWeekStartNav = $dbNavNavs[$forWeek]['nav'];
            } else {
                $forWeek = $totalNavs;
                $forWeekStartNav = $dbNavNavs[0]['nav'];
            }

            $dbNav['navs_chunks']['week'] = [];
            for ($forWeek; $forWeek < $totalNavs; $forWeek++) {
                $dbNav['navs_chunks']['week'][$dbNavNavs[$forWeek]['date']] = [];
                $dbNav['navs_chunks']['week'][$dbNavNavs[$forWeek]['date']]['date'] = $dbNavNavs[$forWeek]['date'];
                $dbNav['navs_chunks']['week'][$dbNavNavs[$forWeek]['date']]['nav'] = $dbNavNavs[$forWeek]['nav'];
                $dbNav['navs_chunks']['week'][$dbNavNavs[$forWeek]['date']]['diff'] =
                    numberFormatPrecision($dbNavNavs[$forWeek]['nav'] - $forWeekStartNav, 4);
                $dbNav['navs_chunks']['week'][$dbNavNavs[$forWeek]['date']]['diff_percent'] =
                    numberFormatPrecision(($dbNavNavs[$forWeek]['nav'] * 100 / $forWeekStartNav - 100), 2);
            }
        }

        if ($totalNavs > 7) {
            if ($totalNavs > 30) {
                $forMonth = $totalNavs - 30;
                $forMonthStartNav = $dbNavNavs[$forMonth]['nav'];
            } else {
                $forMonth = $totalNavs;
                $forMonthStartNav = $dbNavNavs[0]['nav'];
            }

            $dbNav['navs_chunks']['month'] = [];
            for ($forMonth; $forMonth < $totalNavs; $forMonth++) {
                $dbNav['navs_chunks']['month'][$dbNavNavs[$forMonth]['date']] = [];
                $dbNav['navs_chunks']['month'][$dbNavNavs[$forMonth]['date']]['date'] = $dbNavNavs[$forMonth]['date'];
                $dbNav['navs_chunks']['month'][$dbNavNavs[$forMonth]['date']]['nav'] = $dbNavNavs[$forMonth]['nav'];
                $dbNav['navs_chunks']['month'][$dbNavNavs[$forMonth]['date']]['diff'] =
                    numberFormatPrecision($dbNavNavs[$forMonth]['nav'] - $forMonthStartNav, 4);
                $dbNav['navs_chunks']['month'][$dbNavNavs[$forMonth]['date']]['diff_percent'] =
                    numberFormatPrecision(($dbNavNavs[$forMonth]['nav'] * 100 / $forMonthStartNav - 100), 2);
            }
        }

        if ($totalNavs > 30) {
            if ($totalNavs > 365) {
                $forYear = $totalNavs - 365;
                $forYearStartNav = $dbNavNavs[$forYear]['nav'];
            } else {
                $forYear = $totalNavs;
                $forYearStartNav = $dbNavNavs[0]['nav'];
            }

            $dbNav['navs_chunks']['year'] = [];
            for ($forYear; $forYear < $totalNavs; $forYear++) {
                $dbNav['navs_chunks']['year'][$dbNavNavs[$forYear]['date']] = [];
                $dbNav['navs_chunks']['year'][$dbNavNavs[$forYear]['date']]['date'] = $dbNavNavs[$forYear]['date'];
                $dbNav['navs_chunks']['year'][$dbNavNavs[$forYear]['date']]['nav'] = $dbNavNavs[$forYear]['nav'];
                $dbNav['navs_chunks']['year'][$dbNavNavs[$forYear]['date']]['diff'] =
                    numberFormatPrecision($dbNavNavs[$forYear]['nav'] - $forYearStartNav, 4);
                $dbNav['navs_chunks']['year'][$dbNavNavs[$forYear]['date']]['diff_percent'] =
                    numberFormatPrecision(($dbNavNavs[$forYear]['nav'] * 100 / $forYearStartNav - 100), 2);
            }
        }

        if ($totalNavs > 365) {
            if ($totalNavs > 1095) {
                $forThreeYear = $totalNavs - 1095;
                $forThreeYearStartNav = $dbNavNavs[$forThreeYear]['nav'];
            } else {
                $forThreeYear = $totalNavs;
                $forThreeYearStartNav = $dbNavNavs[0]['nav'];
            }

            $dbNav['navs_chunks']['threeYear'] = [];
            for ($forThreeYear; $forThreeYear < $totalNavs; $forThreeYear++) {
                $dbNav['navs_chunks']['threeYear'][$dbNavNavs[$forThreeYear]['date']] = [];
                $dbNav['navs_chunks']['threeYear'][$dbNavNavs[$forThreeYear]['date']]['date'] = $dbNavNavs[$forThreeYear]['date'];
                $dbNav['navs_chunks']['threeYear'][$dbNavNavs[$forThreeYear]['date']]['nav'] = $dbNavNavs[$forThreeYear]['nav'];
                $dbNav['navs_chunks']['threeYear'][$dbNavNavs[$forThreeYear]['date']]['diff'] =
                    numberFormatPrecision($dbNavNavs[$forThreeYear]['nav'] - $forThreeYearStartNav, 4);
                $dbNav['navs_chunks']['threeYear'][$dbNavNavs[$forThreeYear]['date']]['diff_percent'] =
                    numberFormatPrecision(($dbNavNavs[$forThreeYear]['nav'] * 100 / $forThreeYearStartNav - 100), 2);
            }
        }

        if ($totalNavs > 1095) {
            if ($totalNavs > 1825) {
                $forFiveYear = $totalNavs - 1825;
                $forFiveYearStartNav = $dbNavNavs[$forFiveYear]['nav'];
            } else {
                $forFiveYear = $totalNavs;
                $forFiveYearStartNav = $dbNavNavs[0]['nav'];
            }

            $dbNav['navs_chunks']['fiveYear'] = [];
            for ($forFiveYear; $forFiveYear < $totalNavs; $forFiveYear++) {
                $dbNav['navs_chunks']['fiveYear'][$dbNavNavs[$forFiveYear]['date']] = [];
                $dbNav['navs_chunks']['fiveYear'][$dbNavNavs[$forFiveYear]['date']]['date'] = $dbNavNavs[$forFiveYear]['date'];
                $dbNav['navs_chunks']['fiveYear'][$dbNavNavs[$forFiveYear]['date']]['nav'] = $dbNavNavs[$forFiveYear]['nav'];
                $dbNav['navs_chunks']['fiveYear'][$dbNavNavs[$forFiveYear]['date']]['diff'] =
                    numberFormatPrecision($dbNavNavs[$forFiveYear]['nav'] - $forFiveYearStartNav, 4);
                $dbNav['navs_chunks']['fiveYear'][$dbNavNavs[$forFiveYear]['date']]['diff_percent'] =
                    numberFormatPrecision(($dbNavNavs[$forFiveYear]['nav'] * 100 / $forFiveYearStartNav - 100), 2);
            }
        }

        if ($totalNavs > 3652) {
            if ($totalNavs > 3652) {
                $forTenYear = $totalNavs - 3652;
                $forTenYearStartNav = $dbNavNavs[$forTenYear]['nav'];
            } else {
                $forTenYear = $totalNavs;
                $forTenYearStartNav = $dbNavNavs[0]['nav'];
            }

            $dbNav['navs_chunks']['tenYear'] = [];
            for ($forTenYear; $forTenYear < $totalNavs; $forTenYear++) {
                $dbNav['navs_chunks']['tenYear'][$dbNavNavs[$forTenYear]['date']] = [];
                $dbNav['navs_chunks']['tenYear'][$dbNavNavs[$forTenYear]['date']]['date'] = $dbNavNavs[$forTenYear]['date'];
                $dbNav['navs_chunks']['tenYear'][$dbNavNavs[$forTenYear]['date']]['nav'] = $dbNavNavs[$forTenYear]['nav'];
                $dbNav['navs_chunks']['tenYear'][$dbNavNavs[$forTenYear]['date']]['diff'] =
                    numberFormatPrecision($dbNavNavs[$forTenYear]['nav'] - $forTenYearStartNav, 4);
                $dbNav['navs_chunks']['tenYear'][$dbNavNavs[$forTenYear]['date']]['diff_percent'] =
                    numberFormatPrecision(($dbNavNavs[$forTenYear]['nav'] * 100 / $forTenYearStartNav - 100), 2);
            }
        }

        $dbNav['navs_chunks']['all'] = [];
        $forAllStartNav = $dbNavNavs[0]['nav'];
        for ($forAll = 0; $forAll < $totalNavs; $forAll++) {
            $dbNav['navs_chunks']['all'][$dbNavNavs[$forAll]['date']] = [];
            $dbNav['navs_chunks']['all'][$dbNavNavs[$forAll]['date']]['date'] = $dbNavNavs[$forAll]['date'];
            $dbNav['navs_chunks']['all'][$dbNavNavs[$forAll]['date']]['nav'] = $dbNavNavs[$forAll]['nav'];
            $dbNav['navs_chunks']['all'][$dbNavNavs[$forAll]['date']]['diff'] =
                numberFormatPrecision($dbNavNavs[$forAll]['nav'] - $forAllStartNav, 4);
            $dbNav['navs_chunks']['all'][$dbNavNavs[$forAll]['date']]['diff_percent'] =
                numberFormatPrecision(($dbNavNavs[$forAll]['nav'] * 100 / $forAllStartNav - 100), 2);
        }
    }

    protected function fillAmfiNavDays($amfiNavsArr)
    {
        $firstDate = \Carbon\Carbon::parse($this->helper->first($amfiNavsArr)['date']);
        $lastDate = \Carbon\Carbon::parse($this->helper->last($amfiNavsArr)['date']);

        $numberOfDays = $firstDate->diffInDays($lastDate) + 1;//Include last day in calculation

        if ($numberOfDays != count($amfiNavsArr)) {
            $amfiNavs = [];

            foreach ($amfiNavsArr as $amfiNavKey => $amfiNav) {
                $amfiNavs[] = $amfiNav;

                if (isset($amfiNavsArr[$amfiNavKey + 1])) {
                    $currentDate = \Carbon\Carbon::parse($amfiNav['date']);
                    $nextDate = \Carbon\Carbon::parse($amfiNavsArr[$amfiNavKey + 1]['date']);
                    $differenceDays = $currentDate->diffInDays($nextDate);

                    if ($differenceDays > 1) {
                        for ($days = 1; $days < $differenceDays; $days++) {
                            $missingDay = $currentDate->addDay(1)->toDateString();

                            if (!isset($amfiNavs[$missingDay])) {
                                $amfiNav['date'] = $missingDay;

                                array_push($amfiNavs, $amfiNav);
                            }
                        }
                    }
                }
            }

            if ($numberOfDays != count($amfiNavs)) {
                throw new \Exception('Cannot process missing AMFI navs correctly');
            }

            return $amfiNavs;
        }

        return $amfiNavsArr;
    }

    protected function initDb($type, $data = [])
    {
        try {
            $scanDir = $this->basepackages->utils->scanDir($this->destDir, false);

            if ($scanDir && count($scanDir['files']) > 0) {
                foreach ($scanDir['files'] as $file) {
                    if (str_ends_with($file, '-' . $type . '.db')) {
                        try {
                            return (new Sqlite())->init(base_path($file));
                        } catch (\throwable $e) {
                            $this->addResponse('Unable to open database file', 1);

                            return false;
                        }
                    }
                }
            }

            if (count($data) > 0) {
                $type === 'funds';
            }

            //File not exists, redownload
            if (!$this->localContent->fileExists($this->destDir . $this->today . '-' . $type . '.db')) {
                if (count($data) > 0) {
                    $this->addResponse('Download the latest funds file using Extractdata!', 1);

                    return false;
                }

                if ($type === 'latest') {
                    $this->downloadMfNavsData(true);
                } else {
                    $this->downloadMfNavsData(false, true);
                }

                $this->extractMfData();
            }
        } catch (FilesystemException | UnableToCheckExistence | \throwable $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }

        try {
            return (new Sqlite())->init(base_path($this->destDir . $this->today . '-' . $type . '.db'));
        } catch (\throwable $e) {
            $this->addResponse('Unable to open database file', 1);

            return false;
        }
    }

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
        $this->amcsPackage = new MfAmcs;

        $amc = $this->amcsPackage->getMfAmcByName($data['AMC']);

        if (!$amc) {
            $amc = [];
            $amc['name'] = $data['AMC'];

            $amc = $this->amcsPackage->addMfAmcs($amc);

            if ($amc) {
                $amc = [];
                $amc = $this->amcsPackage->packagesData->last;
            }
        }

        return $amc;
    }

    protected function processCategories(array $data)
    {
        $this->categoriesPackage = new MfCategories;

        $categories = explode('-', $data['Scheme Category']);

        if ($categories && (count($categories) === 1 || count($categories) === 2)) {
            array_walk($categories, function(&$category) {
                $category = trim($category);
            });

            $parentCategory = $this->categoriesPackage->getMfCategoryByName($categories[0]);

            if (!$parentCategory) {
                $parentCategory = [];
                $parentCategory['name'] = $categories[0];

                $this->categoriesPackage->addMfCategories($parentCategory);

                $parentCategory = $this->categoriesPackage->packagesData->last;
            }

            if (count($categories) === 2) {
                $childCategory = $this->categoriesPackage->getMfCategoryByName($categories[1]);

                if (!$childCategory) {
                    $childCategory = [];
                    $childCategory['name'] = $categories[1];
                    $childCategory['parent_id'] = $parentCategory['id'];

                    $this->categoriesPackage->addMfCategories($childCategory);

                    $childCategory = $this->categoriesPackage->packagesData->last;
                }

                return $childCategory;
            }

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
<?php

namespace Apps\Fintech\Packages\Mf\Extractdata;

use Apps\Fintech\Packages\Mf\Amcs\MfAmcs;
use Apps\Fintech\Packages\Mf\Categories\MfCategories;
use Apps\Fintech\Packages\Mf\Extractdata\Settings;
use Apps\Fintech\Packages\Mf\Navs\MfNavs;
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

    protected $sourceDir = 'apps/Fintech/Packages/Mf/Extractdata/Data/';

    protected $sourceLink = 'https://github.com/captn3m0/historical-mf-data/releases/latest/download/funds.db.zst';

    protected $trackCounter = 0;

    public $method;

    protected $apiClient;

    protected $apiClientConfig;

    protected $settings = Settings::class;

    protected $navsPackage;

    protected $categoriesPackage;

    protected $amcsPackage;

    public function onConstruct()
    {
        if (!is_dir(base_path($this->sourceDir))) {
            if (!mkdir(base_path($this->sourceDir), 0777, true)) {
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

        $this->now = \Carbon\Carbon::now();

        parent::onConstruct();
    }

    public function __call($method, $arguments)
    {
        if (method_exists($this, $method)) {
            $this->basepackages->progress->updateProgress($method, null, false);

            $call = call_user_func_array([$this, $method], $arguments);

            $callResult = $call;

            if ($call !== false) {
                $call = true;
            }

            $this->basepackages->progress->updateProgress($method, $call, false);

            return $callResult;
        }
    }

    protected function downloadMfData($data)
    {
        $today = $this->now->toDateString();

        try {
            if (isset($data['redownload']) &&
                $data['redownload'] == 'false'
            ) {
                //File is already extracted
                if ($this->localContent->fileExists($this->sourceDir . $today . '-funds.db')) {
                    return true;
                }

                //File is already downloaded
                if ($this->localContent->fileExists($this->sourceDir . $today . '-funds.db.zst')) {
                    return true;
                }
            } else {
                //File is already downloaded
                if ($this->localContent->fileExists($this->sourceDir . $today . '-funds.db.zst')) {
                    $remoteSize = getRemoteFilesize($this->sourceLink);

                    $localSize = $this->localContent->fileSize($this->sourceDir . $today . '-funds.db.zst');

                    if ($remoteSize === $localSize) {
                        return true;
                    }
                }

                if ($this->localContent->fileExists($this->sourceDir . $today . '-funds.db')) {
                    $this->localContent->delete($this->sourceDir . $today . '-funds.db');
                }

                if ($this->localContent->fileExists($this->sourceDir . $today . '-funds.db.zst')) {
                    $this->localContent->delete($this->sourceDir . $today . '-funds.db.zst');
                }
            }
        } catch (FilesystemException | UnableToCheckExistence | UnableToDeleteFile | UnableToRetrieveMetadata | \throwable $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }

        $this->method = 'downloadMfData';

        return $this->downloadData($this->sourceLink, base_path($this->sourceDir) . $today . '-funds.db.zst');
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

                    if ($downloadedBytes === $downloadTotal) {
                        $this->basepackages->progress->updateProgress($this->method, true, false, null, $counters);
                    } else {
                        $this->basepackages->progress->updateProgress($this->method, null, false, null, $counters);
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

    protected function extractMfData($data)
    {
        $this->method = 'extractMfData';

        $today = $this->now->toDateString();

        try {
            if (isset($data['redownload']) &&
                $data['redownload'] == 'false'
            ) {
                //File is already extracted
                if ($this->localContent->fileExists($this->sourceDir . $today . '-funds.db')) {
                    return true;
                }
            } else {
                if ($this->localContent->fileExists($this->sourceDir . $today . '-funds.db')) {
                    $this->localContent->delete($this->sourceDir . $today . '-funds.db');
                }
            }

            $file = $this->localContent->fileExists($this->sourceDir . $today . '-funds.db.zst');

            if (!$file) {
                $this->addResponse('File not downloaded correctly', 1);

                return false;
            }
            //Decompress
            exec('unzstd -d -f ' . base_path($this->sourceDir) . $today . '-funds.db.zst -o ' . base_path($this->sourceDir) . $today . '-funds.db', $output, $result);
            $this->basepackages->progress->updateProgress(
                method: $this->method,
                counters: ['stepsTotal' => 5, 'stepsCurrent' => 1],
                text: 'Decompressing...'
            );

            //Create INDEXES
            exec("echo 'CREATE INDEX \"nav-main\" ON \"nav\" (\"date\",\"scheme_code\")' | sqlite3 " . base_path($this->sourceDir) . $today . "-funds.db", $output, $result);
            $this->basepackages->progress->updateProgress(
                method: $this->method,
                counters: ['stepsTotal' => 5, 'stepsCurrent' => 2],
                text: 'Generating Index...'
            );
            exec("echo 'CREATE INDEX \"nav-scheme\" ON \"nav\" (\"scheme_code\")' | sqlite3 " . base_path($this->sourceDir) . $today . "-funds.db", $output, $result);
            $this->basepackages->progress->updateProgress(
                method: $this->method,
                counters: ['stepsTotal' => 5, 'stepsCurrent' => 3],
                text: 'Generating Index...'
            );
            exec("echo 'CREATE INDEX \"securities-scheme\" ON \"securities\" (\"scheme_code\")' | sqlite3 " . base_path($this->sourceDir) . $today . "-funds.db", $output, $result);
            $this->basepackages->progress->updateProgress(
                method: $this->method,
                counters: ['stepsTotal' => 5, 'stepsCurrent' => 4],
                text: 'Generating Index...'
            );
            exec("echo 'CREATE INDEX \"securities-isin\" ON \"securities\" (\"isin\")' | sqlite3 " . base_path($this->sourceDir) . $today . "-funds.db", $output, $result);
            $this->basepackages->progress->updateProgress(
                method: $this->method,
                counters: ['stepsTotal' => 5, 'stepsCurrent' => 5],
                text: 'Generating Index...'
            );

            if ($result !== 0) {
                $this->addResponse('Error extracting file', 1, ['output' => $output]);

                return false;
            }
        } catch (FilesystemException | UnableToCheckExistence | \throwable $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }

        return true;
    }

    protected function processMfData($data)
    {
        $this->method = 'processMfData';

        $today = $this->now->toDateString();

        try {
            //File is already extracted
            if (!$this->localContent->fileExists($this->sourceDir . $today . '-funds.db')) {
                $this->addResponse('File not downloaded and extracted correctly!', 1);

                return false;
            }
        } catch (FilesystemException | UnableToCheckExistence | \throwable $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }

        try {
            $sqlite = (new Sqlite())->init(base_path($this->sourceDir . $today . '-funds.db'));
        } catch (\throwable $e) {
            $this->addResponse('Unable to open database file', 1);

            return false;
        }

        $this->navsPackage = new MfNavs;

        $isins = $sqlite->query("SELECT * from securities")->fetchAll(Enum::FETCH_ASSOC);

        $isinsTotal = count($isins);

        if ($isins && $isinsTotal > 0) {
            foreach ($isins as $key => $isin) {
                $this->basepackages->utils->setMicroTimer('Start');

                $dbIsin = $this->navsPackage->getMfNavsByIsin($isin['isin']);

                $lastUpdated = $this->now->subDay(1)->toDateString();

                if (isset($data['reset']) && $data['reset'] == 'true') {
                    $lastUpdated = '2000-01-01';
                }

                if (!$dbIsin) {
                    $dbIsin = [];
                    $dbIsin['type'] = $isin['type'];
                    $dbIsin['scheme_code'] = $isin['scheme_code'];
                    $dbIsin['isin'] = $isin['isin'];
                    $dbIsin['navs'] = [];
                } else {
                    $lastUpdated = $dbIsin['last_updated'];
                }

                $isin = $isin['isin'];

                $isinNavs =
                    $sqlite->query(
                        "SELECT * from nav N
                        JOIN securities S ON N.scheme_code = S.scheme_code
                        WHERE S.isin = '$isin'
                        AND N.date >= '$lastUpdated'
                        ORDER BY N.date ASC"
                    )->fetchAll(Enum::FETCH_ASSOC);

                if ($isinNavs && count($isinNavs) > 0) {
                    if ($this->helper->last($isinNavs)['date']) {
                        $dbIsin['last_updated'] = $this->helper->last($isinNavs)['date'];
                    } else {
                        $dbIsin['last_updated'] = $today;
                    }
                    if ($this->helper->last($isinNavs)['nav']) {
                        $dbIsin['latest_nav'] = $this->helper->last($isinNavs)['nav'];
                    } else {
                        $dbIsin['latest_nav'] = 0;
                    }
                    foreach ($isinNavs as $isinNav) {
                        $dbIsin['navs'][$isinNav['date']] = $isinNav['nav'];
                    }
                } else {
                    $dbIsin['last_updated'] = $today;
                    $dbIsin['latest_nav'] = 0;
                }

                if (isset($dbIsin['id'])) {
                    $this->navsPackage->update($dbIsin);
                } else {
                    $this->navsPackage->add($dbIsin);
                }

                $this->basepackages->utils->setMicroTimer('End');

                $time = $this->basepackages->utils->getMicroTimer();

                if ($time && isset($time[1]['difference']) && $time[1]['difference'] !== 0) {
                    $totalTime = date("H:i:s", floor($time[1]['difference'] * ($isinsTotal - $key)));
                }

                $this->basepackages->utils->resetMicroTimer();

                $this->basepackages->progress->updateProgress(
                    method: $this->method,
                    counters: ['stepsTotal' => $isinsTotal, 'stepsCurrent' => ($key + 1)],
                    text: 'Time remaining : ' . $totalTime . '...'
                );
            }
        }

        return true;
    }

    public function sync($data)
    {
        if ($data['sync'] === 'gold') {
            return $this->processGold($data);
        }

        if (!$this->initApi($data)) {
            $this->addResponse('Could not initialize the API.', 1);

            return false;
        }

        $collection = 'MutualFundsApi';
        $method = 'getFund' . ucfirst($data['sync']);

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

        $responseArr = $this->apiClient->useMethod($collection, $method, [])->getResponse(true);

        if ($responseArr && count($responseArr) > 0) {
            $process = 'process' . ucfirst($data['sync']);

            $this->$process($responseArr);

            return true;
        }

        $this->addResponse('Error processing sync', 1);

        return false;
    }

    protected function processCategories(array $responseArr)
    {
        $this->categoriesPackage = new MfCategories;

        $counter = [];
        $counter['new'] = 0;
        $counter['updated'] = 0;

        foreach ($responseArr as $response) {
            $category = $this->categoriesPackage->getMfCategoryByName($response['category_name']);

            if ($category) {
                if ($category['report_date'] !== $response['report_date']) {
                    $category = array_replace($category, $response);

                    $this->categoriesPackage->update($category);

                    $counter['updated'] = $counter['updated'] + 1;
                }
            } else {
                $category = $response;
                $category['name'] = $category['category_name'];

                $this->categoriesPackage->add($category);

                $counter['new'] = $counter['new'] + 1;
            }
        }

        $this->addResponse('Synced categories. Added ' . $counter['new'] . ' categories. Updated ' . $counter['updated'] . ' categories.');

        return true;
    }

    protected function processAmcs(array $responseArr)
    {
        $this->amcsPackage = new MfAmcs;

        $counter = [];
        $counter['new'] = 0;
        $counter['updated'] = 0;

        foreach ($responseArr as $response) {
            $amc = $this->amcsPackage->getMfAmcByCode($response['AMC_code']);

            if ($amc) {
                $amc = array_replace($amc, $response);

                $this->amcsPackage->update($amc);

                $counter['updated'] = $counter['updated'] + 1;
            } else {
                $amc = $response;
                $amc['amc_code'] = $amc['AMC_code'];

                $this->amcsPackage->add($amc);

                $counter['new'] = $counter['new'] + 1;
            }
        }

        $this->addResponse('Synced AMCs. Added ' . $counter['new'] . ' AMCs. Updated ' . $counter['updated'] . ' AMCs.');

        return true;
    }

    protected function processSchemes(array $responseArr)
    {
        trace([$responseArr]);
    }

    protected function processGold($data)
    {
        $today = $this->now->toDateString();

        try {
            if ($this->localContent->fileExists($this->sourceDir . 'gold-' . $today . '.json')) {
                $this->addResponse('File for ' . $today . ' already exists and imported.', 1);

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
                $this->localContent->write($this->sourceDir . 'gold-' . $today . '.json', $response->getBody()->getContents());
            } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                $this->addResponse($e->getmessage(), 1);

                return false;
            }
        } else {
            $this->addResponse($response->getStatusCode() . ':' . $response->getMessage(), 1);

            return false;
        }

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
        }

        if (count($apisArr) > 0) {
            foreach ($apisArr as $api) {
                if ($api['category'] === 'repos') {
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

                        $apis[$api['id']]['id'] = $apiConfig['id'];
                        $apis[$api['id']]['name'] = $apiConfig['name'];
                        $apis[$api['id']]['data']['url'] = $apiConfig['repo_url'];
                    }
                }
            }
        }

        if ($returnApis) {
            return $apis ?? [];
        }

        return $apisArr;
    }
}
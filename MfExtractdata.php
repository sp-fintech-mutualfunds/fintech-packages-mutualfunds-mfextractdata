<?php

namespace Apps\Fintech\Packages\Mf\Extractdata;

use Apps\Fintech\Packages\Mf\Extractdata\Settings;
use League\Flysystem\FilesystemException;
use League\Flysystem\UnableToCheckExistence;
use League\Flysystem\UnableToWriteFile;
use System\Base\BasePackage;

class MfExtractdata extends BasePackage
{
    protected $sourceDir = 'apps/Fintech/Packages/Mf/Extractdata/Data/';

    protected $sourceLink = 'https://github.com/captn3m0/historical-mf-data/releases/latest/download/funds.db.zst';

    protected $trackCounter = 0;

    public $method;

    protected $apiClient;

    protected $apiClientConfig;

    protected $settings = Settings::class;

    public function onConstruct()
    {
        if (!is_dir(base_path($this->sourceDir))) {
            if (!mkdir(base_path($this->sourceDir), 0777, true)) {
                return false;
            }
        }

        // /etc/apache2.conf - Change the timeout to 3600 else you will get Gateway Timeout, revert back when done to 300 (5 mins)
        // Timeout 3600

        //Increase Exectimeout to 20 mins as this process takes time to extract and merge data.
        if ((int) ini_get('max_execution_time') < 3600) {
            set_time_limit(3600);
        }

        //Increase memory_limit to 2G as the process takes a bit of memory to process the array.
        if ((int) ini_get('memory_limit') < 2048) {
            ini_set('memory_limit', '2048M');
        }

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

    protected function downloadMfData($reDownload = false)
    {
        if (!$reDownload) {
            try {
                $file = $this->localContent->fileExists($this->sourceDir . 'funds.db.zst');

                if ($file) {
                    $fileModificationTime = $this->localContent->lastModified($this->sourceDir . 'funds.db.zst');

                    if (\Carbon\Carbon::now()->timestamp < \Carbon\Carbon::parse($fileModificationTime)->addDay()->timestamp) {
                        $this->addResponse('File was downloaded less than a day ago. Please use redownload to force redownload.', 1);

                        return false;
                    }
                }
            } catch (FilesystemException | UnableToCheckExistence | UnableToRetrieveMetadata | \throwable $e) {
                $this->addResponse($e->getMessage(), 1);

                return false;
            }
        }

        $this->method = 'downloadMfData';

        return $this->downloadData($this->sourceLink, base_path($this->sourceDir) . 'funds.db.zst');
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

    protected function extractMfData()
    {
        try {
            $file = $this->localContent->fileExists($this->sourceDir . 'funds.db.zst');

            if (!$file) {
                $this->addResponse('File not downloaded correctly', 1);

                return false;
            }
            exec('unzstd -d -f ' . base_path($this->sourceDir) . 'funds.db.zst -o ' . base_path($this->sourceDir) . 'funds.db', $output, $result);

            if ($result !== 0) {
                $this->addResponse('Error extracting file', 1, ['output' => $output]);

                return false;
            }
        } catch (FilesystemException | UnableToCheckExistence | \throwable $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }
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

        $process = 'process' . ucfirst($data['sync']);

        $this->$process($responseArr);
    }

    protected function processCategories($responseArr)
    {
        trace([$responseArr]);
    }

    protected function processAmcs($responseArr)
    {
        trace([$responseArr]);
    }

    protected function processSchemes($responseArr)
    {
        trace([$responseArr]);
    }

    protected function processGold($data)
    {
        $now = \Carbon\Carbon::now();
        $today = $now->toDateString();

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
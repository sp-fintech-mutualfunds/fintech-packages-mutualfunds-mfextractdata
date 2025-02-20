<?php

namespace Apps\Fintech\Packages\Mf\Extractdata;

use League\Flysystem\FilesystemException;
use League\Flysystem\UnableToCheckExistence;
use System\Base\BasePackage;

class MfExtractdata extends BasePackage
{
    protected $sourceDir = 'apps/Fintech/Packages/Mf/Extractdata/Data/';

    protected $sourceLink = 'https://github.com/captn3m0/historical-mf-data/releases/latest/download/funds.db.zst';

    protected $trackCounter = 0;

    public $method;

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

                    if (\Carbon\Carbon::parse($fileModificationTime)->addDay()->timestamp < \Carbon\Carbon::now()->timestamp) {
                        $this->addResponse($e->getMessage(), 1);

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
}
<?php

namespace Apps\Fintech\Packages\Mf\Tools\Extractdata\TaskCalls;

use Apps\Fintech\Packages\Mf\Tools\Extractdata\MfToolsExtractdata;
use System\Base\Providers\BasepackagesServiceProvider\Packages\Workers\Calls;

class ProcessSyncSchemesNavs extends Calls
{
    protected $funcDisplayName = 'Sync Schemes & Navs';

    protected $funcDescription = 'Sync Schemes & Navs from AMFI India.';

    protected $args;

    public function run(array $args = [])
    {
        $thisCall = $this;

        return function() use ($thisCall, $args) {
            $thisCall->updateJobTask(2, $args);

            $this->args = $this->extractCallArgs($thisCall, $args);

            if (!$this->args) {
                return;
            }

            try {
                $mfExtractDataPackage = new MfToolsExtractdata;

                if (isset($this->args['schemes']) &&
                    $this->args['schemes'] == 'true'
                ) {
                    $mfExtractDataPackage->downloadMfData();
                    $mfExtractDataPackage->processMfData();
                }

                if (isset($this->args['downloadnav']) &&
                    $this->args['downloadnav'] == 'true'
                ) {
                    $mfExtractDataPackage->downloadMfData(false, true);
                    $mfExtractDataPackage->extractMfData();
                    $mfExtractDataPackage->processMfData(false, true);
                }
            } catch (\throwable $e) {
                if ($this->config->logs->exceptions) {
                    $this->logger->logExceptions->critical(json_trace($e));
                }

                $thisCall->packagesData->responseMessage = 'Exception: Please check exceptions log for more details.';

                $thisCall->packagesData->responseCode = 1;

                if (isset($mfExtractDataPackage->responseData)) {
                    $thisCall->packagesData->responseData = $mfExtractDataPackage->responseData;
                }

                $this->addJobResult($thisCall->packagesData, $args);

                $thisCall->updateJobTask(3, $args);

                return;
            }

            $thisCall->packagesData->responseMessage = $mfExtractDataPackage->packagesData->responseMessage ?? 'Ok';

            $thisCall->packagesData->responseCode = $mfExtractDataPackage->packagesData->responseCode ?? 0;

            $this->addJobResult($mfExtractDataPackage->packagesData->responseData ?? [], $args);

            $thisCall->updateJobTask(3, $args);
        };
    }
}
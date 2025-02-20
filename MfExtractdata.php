<?php

namespace Apps\Fintech\Packages\Mf\Extractdata;

use System\Base\BasePackage;

class MfExtractdata extends BasePackage
{
    //protected $modelToUse = ::class;

    protected $packageName = 'mfextractdata';

    public $mfextractdata;

    public function getMfExtractdataById($id)
    {
        $mfextractdata = $this->getById($id);

        if ($mfextractdata) {
            //
            $this->addResponse('Success');

            return;
        }

        $this->addResponse('Error', 1);
    }

    public function addMfExtractdata($data)
    {
        //
    }

    public function updateMfExtractdata($data)
    {
        $mfextractdata = $this->getById($id);

        if ($mfextractdata) {
            //
            $this->addResponse('Success');

            return;
        }

        $this->addResponse('Error', 1);
    }

    public function removeMfExtractdata($data)
    {
        $mfextractdata = $this->getById($id);

        if ($mfextractdata) {
            //
            $this->addResponse('Success');

            return;
        }

        $this->addResponse('Error', 1);
    }
}
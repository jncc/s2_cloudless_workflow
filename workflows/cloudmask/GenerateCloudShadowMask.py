import argparse
import json
import logging
import luigi
import os

from cloudmask.cloud_shadow_mask import runPyFmaskShadowMasking
from cloudmask.GenerateCloudmask import GenerateCloudmask

from luigi import LocalTarget
from luigi.util import requires
from fmask.cmdline.sentinel2Stacked import readTopLevelMeta
from osgeo import gdal

log = logging.getLogger('luigi-interface')

@requires(GenerateCloudmask)
class GenerateCloudShadowMask(luigi.Task):
    stateFolder = luigi.Parameter()
    tempFolder = luigi.Parameter()
    outputFolder = luigi.Parameter()
    safeDir = luigi.Parameter()

    keepIntermediates = luigi.BoolParameter(default=False)

    def run(self):
        with self.input().open('r') as i:
            input = json.load(i)

        basename = os.path.basename(self.safeDir)[:-5]  
        outputImage = os.path.join(self.tempFolder, f'{basename}_mask_fmask_cloudshadow.tif')

        fmaskArgs = argparse.Namespace(
            safedir = self.safeDir,
            tmpdir = self.tempFolder,
            verbose = True
        )
        topMeta = readTopLevelMeta(fmaskArgs)

        interimCloudmask = runPyFmaskShadowMasking(
                input['intermediateFiles']['stackedTOA'],
                #inputSatImage,
                input['intermediateFiles']['anglesFile'],
                input['intermediateFiles']['cloudMask'],
                self.tempFolder,
                topMeta.scaleVal,
                log
            )
        
        output = {
            'intermediateFiles': {
                'anglesFile': input['intermediateFiles']['anglesFile'],
                'stackedTOA': input['intermediateFiles']['stackedTOA'],
                'stackedTOARef': input['intermediateFiles']['stackedTOARef'],
                'cloudMask': input['intermediateFiles']['cloudMask'],
                'cloudShadowMask': interimCloudmask
            }   
        }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        infile = os.path.join(self.stateFolder, 'GenerateCloudmask.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)
import json
import logging
import luigi
import os

from cloudmask.MergeOutputMasks import MergeOutputMasks

from luigi import LocalTarget
from luigi.util import requires
from osgeo import gdal
from pathlib import Path

log = logging.getLogger('luigi-interface')

@requires(MergeOutputMasks)
class ReprojectFiles(luigi.Task):
    stateFolder = luigi.Parameter()
    outputFolder = luigi.Parameter()
    safeDir = luigi.Parameter()

    reproject = luigi.BoolParameter(default=False)
    reprojectionEPSG = luigi.Parameter(default='')

    def run(self):
        with self.input().open('r') as i:
            input = json.load(i)
        log.info(self.outputFolder)
        outputFilename = f'{Path(self.safeDir).stem}_osgb_clouds.tif'
        outputFilePath = os.path.join(self.outputFolder, outputFilename)

        if self.reproject and self.reprojectionEPSG:
            log.info(f'Reprojecting output files to {self.reprojectionEPSG}, output will be stored at {outputFilePath}')
            warpOpt = gdal.WarpOptions(
                format='GTiff',
                dstSRS=self.reprojectionEPSG
            )
            gdal.Warp(outputFilePath, input['outputs']['combinedCloudAndShadowMask'], options=warpOpt)
        elif self.reproject and not self.reprojectionEPSG:
            log.error(f'No EPSG code supplied, but reprojection requested')
            return RuntimeError(f'No EPSG code supplied, but reprojection requested')
        else:
            log.info('No reprojection requested')

        output = input
        output['outputs']['reprojectedCombinedCloudAndShadowMask'] = outputFilePath

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        infile = os.path.join(self.stateFolder, 'MergeOutputMasks.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)

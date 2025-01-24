import json
import logging
import luigi
import os

from cloudmask.BufferMasks import BufferMasks
from cloudmask.Defaults import VERSION

from luigi import LocalTarget
from luigi.util import requires
from osgeo import gdal, gdalconst
from pathlib import Path

log = logging.getLogger('luigi-interface')

@requires(BufferMasks)
class MergeOutputMasks(luigi.Task):
    stateFolder = luigi.Parameter()
    tempFolder = luigi.Parameter()
    outputFolder = luigi.Parameter()
    inputPath = luigi.Parameter()

    bufferData = luigi.BoolParameter(default=True)
    keepIntermediates = luigi.BoolParameter(default=False)

    def run(self):
        with self.input().open('r') as i:
            input = json.load(i)
            output = input
            output['outputs'] = {}

        # Get SAFE dir base name to create output stem and create subfolders
        basename = Path(input['inputs']['safeDir']).with_suffix('').name
        # Create output folder directories if required (in the form of {base}/original_projection/{year}/{month}/{day})
        outputImagePath = Path(self.outputFolder).joinpath(VERSION).joinpath('original_projection', basename[11:15], basename[15:17], basename[17:19])
        outputImagePath.mkdir(parents=True, exist_ok=True)
        # Create output filename stem under the temporary working directory
        tempOutputStem = Path(self.tempFolder).joinpath(f'{basename}.CLOUDMASK')
        # Create output filename stem under the created folder structure
        outputImagePath = f'{Path(outputImagePath).joinpath(f'{basename}.CLOUDMASK')}.tif'

        inputCloud = input['intermediateFiles']['cloudMask']
        inputShadow = input['intermediateFiles']['cloudShadowMask']

        if self.bufferData:
            inputCloud = input['intermediateFiles']['buffered']['cloud']
            inputShadow = input['intermediateFiles']['buffered']['shadow']
        
        log.info(f'Merging datafiles -> {inputShadow} | {inputCloud}')

        vrtOptions = gdal.BuildVRTOptions(VRTNodata=0)
        gdal.BuildVRT(f'{tempOutputStem}.vrt', [inputShadow, inputCloud], options=vrtOptions)
        output['intermediateFiles']['combinedCloudAndShadowMaskVRT'] = f'{tempOutputStem}.vrt'

        translateOptions = gdal.TranslateOptions(format='COG', outputType=gdalconst.GDT_Byte, noData=0, creationOptions=['COMPRESS=LZW'], stats=True)
        gdal.Translate(outputImagePath, f'{tempOutputStem}.vrt', options=translateOptions)
        output['outputs']['combinedCloudAndShadowMask'] = outputImagePath

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        infile = os.path.join(self.stateFolder, 'BufferMasks.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)
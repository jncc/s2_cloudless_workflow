import json
import logging
import luigi
import os
import rasterio
import rasterio.merge

from cloudmask.BufferMasks import BufferMasks

from luigi import LocalTarget
from luigi.util import requires
from osgeo import gdal, gdalconst


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
        
        basename = os.path.basename(input['inputs']['safeDir'])[:-5]  
        outputImageStem = os.path.join(self.outputFolder, f'{basename}_clouds')

        inputCloud = input['intermediateFiles']['cloudMask']
        inputShadow = input['intermediateFiles']['cloudShadowMask']

        if self.bufferData:
            inputCloud = input['intermediateFiles']['buffered']['cloud']
            inputShadow = input['intermediateFiles']['buffered']['shadow']
        
        log.info(f'Merging datafiles -> {inputShadow} | {inputCloud}')

        vrtOptions = gdal.BuildVRTOptions(VRTNodata=0)
        gdal.BuildVRT(f'{outputImageStem}.vrt', [inputShadow, inputCloud], options=vrtOptions)

        translateOptions = gdal.TranslateOptions(format='GTiff', outputType=gdalconst.GDT_Byte, noData=0)
        gdal.Translate(f'{outputImageStem}.tif', f'{outputImageStem}.vrt', options=translateOptions)

        output = input
        output['intermediateFiles']['combinedCloudAndShadowMask'] = f'{outputImageStem}.tif'

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        infile = os.path.join(self.stateFolder, 'BufferMasks.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)
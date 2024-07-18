import json
import logging
import luigi
import os
import rasterio
import rasterio.merge

from cloudmask.BufferMasks import BufferMasks

from luigi import LocalTarget
from luigi.util import requires
from osgeo import gdal, gdalconst, osr


log = logging.getLogger('luigi-interface')

@requires(BufferMasks)
class MergeOutputMasks(luigi.Task):
    stateFolder = luigi.Parameter()
    tempFolder = luigi.Parameter()
    outputFolder = luigi.Parameter()
    safeDir = luigi.Parameter()

    keepIntermediates = luigi.BoolParameter(default=False)

    def run(self):
        with self.input().open('r') as i:
            input = json.load(i)
        
        basename = os.path.basename(self.safeDir)[:-5]  
        outputImageStem = os.path.join(self.outputFolder, f'{basename}_clouds')

        vrtOptions = gdal.BuildVRTOptions(VRTNodata=0)
        gdal.BuildVRT(f'{outputImageStem}.vrt', [input['intermediateFiles']['buffered']['shadow'], input['intermediateFiles']['buffered']['cloud']], options=vrtOptions)

        translateOptions = gdal.TranslateOptions(format='GTiff', outputType=gdalconst.GDT_Byte, noData=0)
        gdal.Translate(f'{outputImageStem}.tif', f'{outputImageStem}.vrt', options=translateOptions)

        # with rasterio.open(input['intermediateFiles']['cloudMask']) as ds:
        #     profile = ds.meta.copy()
        #     profile.update(
        #         nodata=0,
        #         dtype=rasterio.uint8,
        #         driver = 'GTiff'
        #     )

        # (merged, transform) = rasterio.merge.merge([input['intermediateFiles']['buffered']['cloud'], input['intermediateFiles']['buffered']['shadow']], nodata=0)

        # with rasterio.open(outputImage, 'w', **profile) as dst:
        #     dst.write(merged.astype(rasterio.uint8))


        output = input
        output['outputs'] = {
            'combinedCloudAndShadowMask': f'{outputImageStem}.tif'
        }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        infile = os.path.join(self.stateFolder, 'BufferMasks.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)
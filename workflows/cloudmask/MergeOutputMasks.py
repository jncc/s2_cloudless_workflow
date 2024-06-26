import json
import logging
import luigi
import os
import rasterio
import rasterio.merge

from cloudmask.GenerateCloudShadowMask import GenerateCloudShadowMask

from luigi import LocalTarget
from luigi.util import requires
from osgeo_utils import gdal_merge


log = logging.getLogger('luigi-interface')

@requires(GenerateCloudShadowMask)
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
        outputImage = os.path.join(self.outputFolder, f'{basename}_clouds.tif')

        with rasterio.open(input['intermediateFiles']['cloudMask']) as ds:
            profile = ds.meta.copy()
            profile.update(
                nodata=0,
                dtype=rasterio.uint8,
                driver = 'GTiff'
            )

        (merged, transform) = rasterio.merge.merge([input['intermediateFiles']['cloudShadowMask'], input['intermediateFiles']['cloudMask']], nodata=0)

        with rasterio.open(outputImage, 'w', **profile) as dst:
            dst.write(merged.astype(rasterio.uint8))


        output = input
        output['outputs'] = {
            'combinedCloudAndShadowMask': outputImage
        }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        infile = os.path.join(self.stateFolder, 'GenerateCloudShadowMask.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)
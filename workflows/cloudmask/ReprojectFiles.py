import json
import logging
import luigi
import os

from cloudmask.MergeOutputMasks import MergeOutputMasks
from cloudmask.operations.reprojection import getEPSGCodeFromProjection, getReprojectedBoundingBox, getBoundBoxPinnedToGrid

from luigi import LocalTarget
from luigi.util import requires
from osgeo import gdal, osr
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

        output = input
        output['outputs'] = {}

        log.info(self.outputFolder)
        outputFilename = f'{Path(self.safeDir).stem}_osgb_clouds.tif'
        outputFilePath = os.path.join(self.outputFolder, outputFilename)

        if self.reproject and self.reprojectionEPSG:
            log.info(f'Reprojecting output files to {self.reprojectionEPSG}, output will be stored at {outputFilePath}')
            
            sourceFile = gdal.Open(input['intermediateFiles']['combinedCloudAndShadowMask'], gdal.GA_ReadOnly)
            (xUpperLeft, xResolution, xSkew, yUpperLeft, ySkew, yResolution) = sourceFile.GetGeoTransform()
            xLowerRight = xUpperLeft + (sourceFile.RasterXSize * xResolution)
            yLowerRight = yUpperLeft + (sourceFile.RasterYSize * yResolution)

            inputProjection = osr.SpatialReference(sourceFile.GetProjection())
            
            code = int(self.reprojectionEPSG)
            outputProjection = osr.SpatialReference()
            outputProjection.ImportFromEPSG(code)

            (xMin, yMin, xMax, yMax) = getReprojectedBoundingBox(min(xUpperLeft, xLowerRight), min(yUpperLeft, yLowerRight), max(xUpperLeft, xLowerRight), max(yUpperLeft, yLowerRight), inputProjection, outputProjection)
            (xPinnedMin, yPinnedMin, xPinnedMax, yPinnedMax) = getBoundBoxPinnedToGrid(xMin, yMin, xMax, yMax, xResolution, yResolution)

            warpOpt = gdal.WarpOptions(
                format='GTiff',
                dstSRS=f'EPSG:{self.reprojectionEPSG}',
                dstNodata=0,
                xRes=xResolution,
                yRes=yResolution,
                outputBounds=(xPinnedMin, yPinnedMin, xPinnedMax, yPinnedMax)
            )
            gdal.Warp(outputFilePath, input['intermediateFiles']['combinedCloudAndShadowMask'], options=warpOpt)
            output['outputs']['reprojectedCombinedCloudAndShadowMask'] = outputFilePath
        elif self.reproject and not self.reprojectionEPSG:
            log.error(f'No EPSG code supplied, but reprojection requested')
            return RuntimeError(f'No EPSG code supplied, but reprojection requested')
        else:
            log.info('No reprojection requested')
            output['outputs']['combinedCloudAndShadowMask'] = input['intermediateFiles']['combinedCloudAndShadowMask']

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        infile = os.path.join(self.stateFolder, 'MergeOutputMasks.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)

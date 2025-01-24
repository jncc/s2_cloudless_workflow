import json
import logging
import luigi
import os

from cloudmask.MergeOutputMasks import MergeOutputMasks
from cloudmask.operations.reprojection import getReprojectedBoundingBox, getBoundBoxPinnedToGrid
from cloudmask.Defaults import VERSION

from luigi import LocalTarget
from luigi.util import requires
from osgeo import gdal, gdalconst, osr
from pathlib import Path
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

log = logging.getLogger('luigi-interface')

@requires(MergeOutputMasks)
class ReprojectFiles(luigi.Task):
    stateFolder = luigi.Parameter()
    tempFolder = luigi.Parameter()
    outputFolder = luigi.Parameter()
    inputPath = luigi.Parameter()

    reproject = luigi.BoolParameter(default=False)
    reprojectionEPSG = luigi.Parameter(default='')

    def run(self):
        with self.input().open('r') as i:
            input = json.load(i)
            output = input

        if self.reproject and self.reprojectionEPSG:
            # Get SAFE dir base name to create output stem and create subfolders
            basename = Path(input['inputs']['safeDir']).with_suffix('').name
            # Create output folder directories if required (in the form of {base}/reprojected/{year}/{month}/{day})
            outputImagePath = Path(self.outputFolder).joinpath(VERSION).joinpath('reprojected', f'epsg_{self.reprojectionEPSG}', basename[11:15], basename[15:17], basename[17:19])
            outputImagePath.mkdir(parents=True, exist_ok=True)

            outputFilename = f'{Path(input['inputs']['safeDir']).stem}.EPSG_{self.reprojectionEPSG}.CLOUDMASK.tif'
            outputFilePath = os.path.join(outputImagePath, outputFilename)
            log.info(f'Reprojecting output files to {self.reprojectionEPSG}, output will be stored at {outputFilePath}')
            
            sourceFile = gdal.Open(input['outputs']['combinedCloudAndShadowMask'], gdal.GA_ReadOnly)
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

            intermediateFilePath = Path(self.tempFolder).joinpath(outputFilename)
            gdal.Warp(f'{intermediateFilePath}', input['outputs']['combinedCloudAndShadowMask'], options=warpOpt)
            output['intermediateFiles']['intermediateReprojectedFile'] = f'{intermediateFilePath}'

            cog_translate(source=intermediateFilePath, dst_path=outputFilePath, dst_kwargs=cog_profiles.get("deflate"), forward_band_tags=True, use_cog_driver=True)
            output['outputs']['reprojectedCombinedCloudAndShadowMask'] = f'{outputFilePath}'
        elif self.reproject and not self.reprojectionEPSG:
            log.error(f'No EPSG code supplied, but reprojection requested')
            return RuntimeError(f'No EPSG code supplied, but reprojection requested')
        else:
            log.info('No reprojection requested')

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def input(self):
        infile = os.path.join(self.stateFolder, 'MergeOutputMasks.json')
        return LocalTarget(infile)

    def output(self):
        outFile = os.path.join(self.stateFolder, f'{type(self).__name__}.json')
        return LocalTarget(outFile)

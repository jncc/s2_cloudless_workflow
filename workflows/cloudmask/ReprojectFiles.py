import json
import logging
import luigi
import os
import shapely

from cloudmask.MergeOutputMasks import MergeOutputMasks

from luigi import LocalTarget
from luigi.util import requires
from osgeo import gdal, gdalconst, ogr, osr
from pathlib import Path
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

log = logging.getLogger('luigi-interface')

@requires(MergeOutputMasks)
class ReprojectFiles(luigi.Task):
    stateFolder = luigi.Parameter()
    workingFolder = luigi.Parameter()
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

            outputFilename = f'{Path(input['inputs']['safeDir']).stem}.EPSG_{self.reprojectionEPSG}.CLOUDMASK.tif'
            outputFilePath = os.path.join(self.workingFolder, f'final_{outputFilename}')
            log.info(f'Reprojecting output files to {self.reprojectionEPSG}, output will be stored at {outputFilePath}')
            
            sourceFile = gdal.Open(input['intermediateFiles']['combinedCloudAndShadowMask'], gdal.GA_ReadOnly)
            # TODO: We assume are using the same unit in input and reprojected outputs, should really check this to make
            # it generic and translate between different types of units
            (xUpperLeft, xResolution, xSkew, yUpperLeft, ySkew, yResolution) = sourceFile.GetGeoTransform()
            
            code = int(self.reprojectionEPSG)
            outputProjection = osr.SpatialReference()
            outputProjection.ImportFromEPSG(code)
            outputProjectionExtent = outputProjection.GetAreaOfUse()
            outputProjectionCutline = f'POLYGON(({outputProjectionExtent.west_lon_degree} {outputProjectionExtent.south_lat_degree}, {outputProjectionExtent.west_lon_degree} {outputProjectionExtent.north_lat_degree}, {outputProjectionExtent.east_lon_degree} {outputProjectionExtent.north_lat_degree}, {outputProjectionExtent.east_lon_degree} {outputProjectionExtent.south_lat_degree}, {outputProjectionExtent.west_lon_degree} {outputProjectionExtent.south_lat_degree}))'
            outputProjectionCutlinePolygon = ogr.CreateGeometryFromWkt(outputProjectionCutline)
            outputProjectionCutlinePolygon.FlattenTo2D()

            # Get the bounds of the input file
            (ulx, xres, xskew, uly, yskew, yres) = sourceFile.GetGeoTransform()
            lrx = ulx + (sourceFile.RasterXSize * xres)
            lry = uly + (sourceFile.RasterYSize * yres)
            
            # Create a polygon from the bounds of the input file
            minX = min(lrx, ulx)
            maxX = max(lrx, ulx)
            minY = min(lry, uly)
            maxY = max(lry, uly)

            ring = ogr.Geometry(ogr.wkbLinearRing)
            ring.AddPoint(minX, minY)
            ring.AddPoint(maxX, minY)
            ring.AddPoint(maxX, maxY)
            ring.AddPoint(minX, maxY)
            ring.AddPoint(minX, minY)

            bounds = ogr.Geometry(ogr.wkbPolygon)
            bounds.AddGeometry(ring)
            bounds.FlattenTo2D()

            # Reproject the bounds to EPSG:4326
            sourcePrj = osr.SpatialReference()
            sourcePrj.ImportFromWkt(sourceFile.GetProjectionRef())
            targetPrj = osr.SpatialReference()
            targetPrj.ImportFromEPSG(4326)
            # Force lat / lon order
            targetPrj.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
            transform = osr.CoordinateTransformation(sourcePrj, targetPrj)
            bounds.Transform(transform)

            if bounds.Within(outputProjectionCutlinePolygon):
                # If bounds of data are completely within the output projection extent, we can use the whole extent
                log.info(f'Source data is within the output projection extent, no cutline needed')
                warpOpt = gdal.WarpOptions(
                    format='GTiff',
                    dstSRS=f'EPSG:{self.reprojectionEPSG}',
                    dstNodata=0,
                    resampleAlg=gdalconst.GRA_Bilinear,
                    targetAlignedPixels=True,
                    multithread=True,
                    xRes=xResolution,
                    yRes=yResolution
                )
            elif bounds.Intersects(outputProjectionCutlinePolygon):
                # If the bounds of the data intersect with the output projection extent, we need to create a cutline
                log.info(f'Source data intersects with the output projection extent, creating cutline')
                # Intersect with the projection extent and buffer a little to avoid some reporjection issues
                boundsClipped = bounds.Intersection(outputProjectionCutlinePolygon)
                cutline = shapely.from_wkt(boundsClipped.ExportToWkt())
                # Buffer cutline a little to avoid reprojection issues
                cutlineBuffered = cutline.buffer(0.01, join_style='mitre')

                # Crop output warped image to the cutline bounds to create output images
                warpOpt = gdal.WarpOptions(
                    format='GTiff',
                    dstSRS=f'EPSG:{self.reprojectionEPSG}',
                    dstNodata=0,
                    cutlineWKT=cutlineBuffered.wkt,
                    cutlineSRS='EPSG:4326',
                    cropToCutline=True,
                    resampleAlg=gdalconst.GRA_Bilinear,
                    targetAlignedPixels=True,
                    multithread=True,
                    xRes=xResolution,
                    yRes=yResolution
                )
            else:
                # If the bounds of the data do not intersect with the output projection extent, we should not reproject
                # TODO: Bump this to a warning?
                log.error(f'Output projection extent does not intersect with source data extent, would produce an empty file')
                raise RuntimeError(f'Output projection extent does not intersect with source data extent')
            
            intermediateFilePath = Path(self.workingFolder).joinpath(outputFilename)
            gdal.Warp(f'{intermediateFilePath}', input['intermediateFiles']['combinedCloudAndShadowMask'], options=warpOpt)
            output['intermediateFiles']['intermediateReprojectedFile'] = f'{intermediateFilePath}'

            cog_translate(source=intermediateFilePath, dst_path=outputFilePath, dst_kwargs=cog_profiles.get("deflate"), forward_band_tags=True, use_cog_driver=True)
            output['intermediateFiles']['reprojectedCombinedCloudAndShadowMask'] = f'{outputFilePath}'
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

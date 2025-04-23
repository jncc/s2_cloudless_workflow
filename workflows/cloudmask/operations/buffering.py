import os
from osgeo import gdal,gdalconst,ogr
from pathlib import Path

def polygonizeData(tempFolder: str, basename: str, type: str, inputRaster: str, inputBand: int = 1):
    outputPath = os.path.join(tempFolder, f'{basename}_{type}.gpkg')

    driver = ogr.GetDriverByName('GPKG')
    dst = driver.CreateDataSource(outputPath)

    inputDS = gdal.Open(inputRaster)

    layer = dst.CreateLayer(type, geom_type=ogr.wkbPolygon, srs=inputDS.GetSpatialRef())
    layer.CreateField(ogr.FieldDefn(type, ogr.OFTInteger))
    fld = layer.GetLayerDefn().GetFieldIndex(type)

    gdal.Polygonize(inputDS.GetRasterBand(inputBand), None, layer, fld, [], callback=None)

    return outputPath

def bufferData(vector: str, tempFolder: str, layerName: str, fieldName: str, fieldValue: int = 1, outputFieldValue: int = 1, bufferDist: int = 100, createNewVectorFile: bool = False):
    dst = ogr.Open(vector, 1)

    # Filter out no data to return only masked data (i.e. data with value of 1)
    inputLayer = dst.GetLayer(layerName)
    filteredInputLayer = dst.ExecuteSQL(f'SELECT * FROM {layerName} WHERE {fieldName} = {fieldValue}')

    # Set output file path and field name
    outputVector = vector
    outputLayerName = f'{layerName}_buffered'
    outputFieldName = f'{layerName}_buffered'
    outputDst = dst

    # If we are creating a new file to store the output file, then create it and update the relevant output variables
    if createNewVectorFile:
        outputVector = os.path.join(tempFolder, f'{Path(vector).stem}_buffered.gpkg')
        driver = ogr.GetDriverByName('GPKG')
        outputDst = driver.CreateDataSource(outputVector)

    # Create new layer in data file        
    layer = outputDst.CreateLayer(outputLayerName, geom_type=ogr.wkbPolygon, srs=inputLayer.GetSpatialRef())
    layer.CreateField(ogr.FieldDefn(outputFieldName, ogr.OFTInteger))
    featureDefn = layer.GetLayerDefn()

    # Buffer and save filtered geometries from the input layer
    for feature in filteredInputLayer:
        inGeom = feature.GetGeometryRef()
        bufferedGeom = inGeom.Buffer(bufferDist)

        outFeature = ogr.Feature(featureDefn)
        outFeature.SetGeometry(bufferedGeom)
        outFeature.SetFID(feature.GetFID())
        outFeature.SetField(outputFieldName, outputFieldValue)
        layer.CreateFeature(outFeature)
        outFeature = None

    return (outputVector, outputLayerName, outputFieldName)

def rasterizeData(inputVector: str, inputLayerName: str, inputAttributeName: str, originalSource: str, tempFolder: str, basename: str, type: str):
    inputDatasource = ogr.Open(inputVector, gdalconst.GA_ReadOnly)
    inputLayer = inputDatasource.GetLayer(inputLayerName)

    originalDatasource = gdal.Open(originalSource, gdalconst.GA_ReadOnly)
    originalGeoTransform = originalDatasource.GetGeoTransform()
    originalXRes = originalDatasource.RasterXSize
    originalYRes = originalDatasource.RasterYSize

    outputPath = os.path.join(tempFolder, f'{basename}_{type}.tif')
    outputDriver = gdal.GetDriverByName('GTiff')
    outputDatasource = outputDriver.Create(outputPath, originalXRes, originalYRes, 1 ,gdal.GDT_Byte)
    outputDatasource.SetGeoTransform(originalGeoTransform)
    outputDatasource.SetProjection(originalDatasource.GetProjection())
    outputBand = outputDatasource.GetRasterBand(1)
    outputNoDataValue = 0
    
    outputBand.SetNoDataValue(outputNoDataValue)
    outputBand.FlushCache()

    gdal.RasterizeLayer(outputDatasource, [1], inputLayer, options=[f'ATTRIBUTE={inputAttributeName}']) 

    return outputPath

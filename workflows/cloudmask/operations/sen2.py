import argparse
import fmask.cmdline.sentinel2Stacked
import fmask.config
import fmask.fmask
import logging
import numpy
import os
import rasterio

from osgeo import gdal
from pathlib import Path
from fmask import config

def generateStackedImageAndAnglesFile(safeDir:str, outDir: str, pixelSize: int, log:logging.Logger):
    """Generates a band stacked image and assocaited angles file from an input
    SAFE dir, uses functiosn from the python FMASK package to generate these 
    intermediate files for use with S2 Cloudless processing and to then use
    with python FMASK to generate the associated cloudmask.

    Args:
        safeDir (str): Path to the unzipped input Sentinel 2 SAFE dir
        outDir (str): Path to store any associated outputs from this process
        pixelSize (int): The pixel size that we want to resample the input 
        bands into

    Returns:
        (str, str): Returns a tuple containing a Stacked input image generated 
        from the supplied SAFE dir and an assoicated Angles image path in the 
        form; (stackedTOAPath, anglesImagePath)
    """
    log.info(f'Generating Stacked Input Image and Angles Image from input safe dir: {safeDir}')
    fmaskArgs = argparse.Namespace(
        pixsize = pixelSize,
        granuledir = None,
        safedir = safeDir,
        tempdir = outDir,
        verbose = True
    )
    
    fmask.cmdline.sentinel2Stacked.makeStackAndAngles(fmaskArgs)
    anglesFile = fmask.cmdline.sentinel2Stacked.checkAnglesFile(fmaskArgs.anglesfile, fmaskArgs.toa)
   
    return (fmaskArgs.toa, anglesFile)

def makeRefOffsetDict(topMeta):
    """
    Take the given sen2meta.Sen2ZipfileMeta object and convert it
    into a dictionary suitable to give to FmaskConfig.setTOARefOffsetDict.

    """
    bandIndexNameDict = {
        0: "B01", 
        1: "B02",
        2: "B03",
        3: "B04",
        4: "B05", 
        5: "B06", 
        6: "B07", 
        7: "B08", 
        8: "B08A", 
        9: "B09", 
        10: "B10", 
        11: "B11", 
        12: "B12"
    }

    offsetDict = {}
    for bandNdx in bandIndexNameDict:
        bandNameStr = bandIndexNameDict[bandNdx]
        if bandNameStr in topMeta.offsetValDict:
            offsetVal = topMeta.offsetValDict[bandNameStr]
        else:
            offsetVal = 0
        offsetDict[bandNdx] = offsetVal
    
    return offsetDict

def refDNtoUnits(refDN, scaleVal, offsetDict):
    """
    Convert the given reflectance pixel value array to physical units,
    using parameters given in fmaskConfig. 

    Scaling is ref = (dn+offset)/scaleVal

    """
    refUnits = numpy.zeros(refDN.shape, dtype=numpy.float32)
    numBands = refDN.shape[0]

    print(offsetDict)
    for bandNdx in range(numBands):
        offset = 0

        if bandNdx in offsetDict:
            offset = offsetDict[bandNdx]
        print(offset)
        refUnits[bandNdx] = singleRefDNtoUnits(refDN[bandNdx], scaleVal, offset)
    return refUnits


def singleRefDNtoUnits(refDN, scaleVal, offset):
    """
    Apply the given scale and offset to transform a single band
    of reflectance from digital number (DN) to reflectance units.

    Calculation is
        ref = (refDN + offset) / scaleVal
    """
    ref = (numpy.float32(refDN) + offset) / scaleVal
    return ref

def generateTOAReflectanceDN(stackedTOA:str, safeDir:str, outDir:str, log:logging.Logger):
    """Generates a Top Of Atmosphere Reflectance image from a stacked input 
    image and a SAFE dir of that image

    Args:
        stackedTOA (str): File path to Stacked TOA input file
        safeDir (str): Path to SAFE directory associated with TOA input file
        outDir (str): Path to temporary directory for processing outputs

    Returns:
        NDArray[floating[_32Bit]]: Numpy Array of stacked input TOA image 
        transformed into reflectance DN values according to its metadata            
    """
    log.info(f'Generating TOA Reflectance DB from stacked input file: {stackedTOA}')

    fmaskArgs = argparse.Namespace(
        safedir = safeDir,
        tmpdir = outDir,
        verbose = True
    )
    topMeta = fmask.cmdline.sentinel2Stacked.readTopLevelMeta(fmaskArgs)
    offsetDict = makeRefOffsetDict(topMeta)
    
    with rasterio.open(stackedTOA, 'r') as ds:
        arr = ds.read().astype(numpy.float32)
        profile = ds.profile

    refDn = refDNtoUnits(arr, topMeta.scaleVal, offsetDict)

    ds = gdal.Open(stackedTOA)

    stackedTOARef = os.path.join(outDir, (f'{Path(stackedTOA).stem}_stacked_toar.tif'))
    profile.update(
        dtype = rasterio.float32
    )

    return writeOutIntermidateFileViaGDAL(refDn, 
                                          stackedTOARef, 
                                          profile,
                                          log)

def writeOutIntermidateFileViaGDAL(numpyArray:numpy.ndarray, outputPath:str, inputProfile, log:logging.Logger, fileFormat:str = 'GTiff'):
    """Writes out a given numpy array to a raster file using RasterIO

    Args:
        numpyArray (numpy.ndarray): NumPy array to write out to raster
        outputPath (str): Output path to write file to
        inputProfile (_type_): RasterIO profile to provider CRS/GeoTransform etc...
        log (logging.Logger): Output logger
        fileFormat (str, optional): Raster format to use, needs to be a valid driver name. Defaults to 'GTiff'.

    Returns:
        str: The path that was written out to
    """
    log.info(f'Writing out intermediate file to {outputPath}')

    inputProfile.update(
        driver = fileFormat
    )

    with rasterio.open(outputPath, 'w', **inputProfile) as output:
        output.write(numpyArray)

    return outputPath
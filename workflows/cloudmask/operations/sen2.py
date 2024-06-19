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

def generateTOAReflectance(stackedTOA:str, safeDir:str, outDir:str, log:logging.Logger):
    """Generates a Top Of Atmosphere Reflectance image from a stacked input 
    image and a SAFE dir of that image

    Args:
        stackedTOA (str): File path to Stacked TOA input file
        safeDir (str): Path to SAFE directory associated with TOA input file
        outDir (str): Path to temporary directory for processing outputs

    Returns:
        NDArray[floating[_32Bit]]: Numpy Array of stacked input TOA image 
        transformed into reflectance values according to its metadata            
    """
    log.info(f'Generating TOA Reflectance from stacked input file: {stackedTOA}')
    
    log.info(f'Converting to reflectance using input metadata from SAFE directory')    

    fmaskArgs = argparse.Namespace(
        safedir = safeDir,
        tmpdir = outDir,
        verbose = True
    )
    topMeta = fmask.cmdline.sentinel2Stacked.readTopLevelMeta(fmaskArgs)

    fmaskConfig = fmask.config.FmaskConfig(fmask.config.FMASK_SENTINEL2)
    fmaskConfig.setTOARefOffsetDict(
        fmask.cmdline.sentinel2Stacked.makeRefOffsetDict(topMeta))
    fmaskConfig.setTempDir(outDir)
    fmaskConfig.setTOARefScaling(topMeta.scaleVal)

    with rasterio.open(stackedTOA, 'r') as ds:
        arr = ds.read().astype(numpy.float32)
        profile = ds.profile

    refDn = fmask.fmask.refDNtoUnits(arr, fmaskConfig)

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
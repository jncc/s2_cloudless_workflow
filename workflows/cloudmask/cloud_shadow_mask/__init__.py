import logging
import os

import fmask.config
import fmask.fmask

def runPyFmaskShadowMasking(
    stackedTOA:str,
    #saturatedPixelMask,
    viewAngles:str,
    cloudMask:str,
    tempDir:str,
    scaleFactor:float,
    log:logging.Logger 
):
    """Generates a cloud shadow mask from a supplied raster cloud mask using FMask

    Args:
        stackedTOA (str): Path to stacked TOA Sentinel 2 image
        viewAngles (str): Path to sun view angles generated from input SAFE dir
        cloudMask (str): Path to cloud mask image (raster 0/1)
        tempDir (str): Path to temporary directory to store intermediate products
        scaleFactor (float): TOA scale factor to reflectance DN units
        log (logging.Logger): Central logger to use

    Returns:
        str: Path to output cloud shadow mask image
    """
    anglesInfo = fmask.config.AnglesFileInfo(
        viewAngles,
        3,
        viewAngles,
        2,
        viewAngles,
        1,
        viewAngles,
        0,
    )

    tmp_base_name = os.path.splitext(os.path.basename(stackedTOA))[0]

    fmaskCloudsImg = os.path.join(
        tempDir, "{}_pyfmask_clouds_result.tif".format(tmp_base_name)
    )
    fmaskFilenames = fmask.config.FmaskFilenames()
    fmaskFilenames.setTOAReflectanceFile(stackedTOA)
    #fmaskFilenames.setSaturationMask(saturatedPixelMask)
    fmaskFilenames.setOutputCloudMaskFile(fmaskCloudsImg)

    fmaskConfig = fmask.config.FmaskConfig(fmask.config.FMASK_SENTINEL2)
    fmaskConfig.setAnglesInfo(anglesInfo)
    fmaskConfig.setKeepIntermediates(True)
    fmaskConfig.setVerbose(True)
    fmaskConfig.setTempDir(tempDir)
    fmaskConfig.setTOARefScaling(float(scaleFactor))
    fmaskConfig.setMinCloudSize(8)
    fmaskConfig.setCloudBufferSize(10)
    fmaskConfig.setShadowBufferSize(10)

    missingThermal = True

    log.info("Cloud layer, pass 1")
    (
        pass1file,
        Twater,
        Tlow,
        Thigh,
        NIR_17,
        nonNullCount,
    ) = fmask.fmask.doPotentialCloudFirstPass(
        fmaskFilenames, fmaskConfig, missingThermal
    )

    log.info("Potential shadows")
    potentialShadowsFile = fmask.fmask.doPotentialShadows(
        fmaskFilenames, fmaskConfig, NIR_17
    )

    log.info("Clumping clouds")
    (clumps, numClumps) = fmask.fmask.clumpClouds(cloudMask)

    log.info("Making 3d clouds")
    (cloudShape, cloudBaseTemp, cloudClumpNdx) = fmask.fmask.make3Dclouds(
        fmaskFilenames, fmaskConfig, clumps, numClumps, missingThermal
    )

    log.info("Making cloud shadow shapes")
    shadowShapesDict = fmask.fmask.makeCloudShadowShapes(
        fmaskFilenames, fmaskConfig, cloudShape, cloudClumpNdx
    )

    log.info("Matching shadows")
    interimShadowmask = fmask.fmask.matchShadows(
        fmaskConfig,
        cloudMask,
        potentialShadowsFile,
        shadowShapesDict,
        cloudBaseTemp,
        Tlow,
        Thigh,
        pass1file,
    )

    return interimShadowmask

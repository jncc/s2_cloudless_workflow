import os

import fmask.config
import fmask.fmask

def run_pyfmask_shadow_masking(
    sen2_toa_img,
    #sen2_sat_img,
    sen2_view_angles_img,
    cloud_msk_img,
    tmp_base_dir,
    toa_img_scale_factor
):
    """

    :param sen2_toa_img:
    :param sen2_sat_img:
    :param sen2_view_angles_img:
    :param cloud_msk_img:
    :param tmp_base_dir:
    :param toa_img_scale_factor:

    """
    anglesInfo = fmask.config.AnglesFileInfo(
        sen2_view_angles_img,
        3,
        sen2_view_angles_img,
        2,
        sen2_view_angles_img,
        1,
        sen2_view_angles_img,
        0,
    )

    tmp_base_name = os.path.splitext(os.path.basename(sen2_toa_img))[0]

    fmaskCloudsImg = os.path.join(
        tmp_base_dir, "{}_pyfmask_clouds_result.tif".format(tmp_base_name)
    )
    fmaskFilenames = fmask.config.FmaskFilenames()
    fmaskFilenames.setTOAReflectanceFile(sen2_toa_img)
    #fmaskFilenames.setSaturationMask(sen2_sat_img)
    fmaskFilenames.setOutputCloudMaskFile(fmaskCloudsImg)

    fmaskConfig = fmask.config.FmaskConfig(fmask.config.FMASK_SENTINEL2)
    fmaskConfig.setAnglesInfo(anglesInfo)
    fmaskConfig.setKeepIntermediates(True)
    fmaskConfig.setVerbose(True)
    fmaskConfig.setTempDir(tmp_base_dir)
    fmaskConfig.setTOARefScaling(float(toa_img_scale_factor))
    fmaskConfig.setMinCloudSize(8)
    fmaskConfig.setCloudBufferSize(10)
    fmaskConfig.setShadowBufferSize(10)

    missingThermal = True

    print("Cloud layer, pass 1")
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

    print("Potential shadows")
    potentialShadowsFile = fmask.fmask.doPotentialShadows(
        fmaskFilenames, fmaskConfig, NIR_17
    )

    print("Clumping clouds")
    (clumps, numClumps) = fmask.fmask.clumpClouds(cloud_msk_img)

    print("Making 3d clouds")
    (cloudShape, cloudBaseTemp, cloudClumpNdx) = fmask.fmask.make3Dclouds(
        fmaskFilenames, fmaskConfig, clumps, numClumps, missingThermal
    )

    print("Making cloud shadow shapes")
    shadowShapesDict = fmask.fmask.makeCloudShadowShapes(
        fmaskFilenames, fmaskConfig, cloudShape, cloudClumpNdx
    )

    print("Matching shadows")
    interimShadowmask = fmask.fmask.matchShadows(
        fmaskConfig,
        cloud_msk_img,
        potentialShadowsFile,
        shadowShapesDict,
        cloudBaseTemp,
        Tlow,
        Thigh,
        pass1file,
    )

    return interimShadowmask

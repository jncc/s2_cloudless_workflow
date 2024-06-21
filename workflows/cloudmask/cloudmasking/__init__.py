from s2cloudless import S2PixelCloudDetector

import logging
import numpy
import rasterio
import os

def generateCloudMask(safeDir: str, stackedTOARef: str, tmpDir: str, outputDir: str, logger:logging.Logger, threshold=0.6, average_over=4, dilation_size=2, all_bands=False):
    """Generates a cloudmask using S2 Cloudless, given an input safe directory

    Args:
        safeDir (str): Path to input SAFE directory, must be unzipped
        stackedTOARef (str): Path to a single stacked raster file of TOA Reflectance converted to DN
        tmpDir (str): Path to a tempory directory used to store intermediate files
        outputDir (str): Path to an output directory to store finished files
        threshold (float, optional): Cloud detection threshold value passed to S2 Cloudless. Defaults to 0.6.
        average_over (int, optional): Cloud detection average_over value passed to S2 Cloudless. Defaults to 4.
        dilation_size (int, optional): Cloud detection dilation_size value passed to S2 Cloudless. Defaults to 2.
        all_bands (bool, optional): Cloud detection value passed to S2 Cloudless, denotes if we are using all bands or the 10 required bands. Defaults to False.
    """
    basename = os.path.basename(safeDir)[:-5]  
    outputFile = os.path.join(outputDir, f'{basename}_mask_s2cloudless_cloud.tif')
    logger.info(f'Output will be written to {outputFile}')
    
    with rasterio.open(stackedTOARef) as ds:
        bands = ds.read()
        meta = ds.meta.copy()

    # required shape needs the band count (10) at the end so juggle the axis around so it looks like: (12066, 12066, 10)
    # for verification, print out the shape of the inputs before and after        
    bands = numpy.moveaxis(bands, [0, 1, 2], [2, 0, 1])   
    # remove the unnecessary bands (B03, B06, B07) as the cloud detector does not work when all 13 bands are passed and flag set to True
    bands = numpy.delete(bands, 2, 2)
    bands = numpy.delete(bands, 4, 2)
    bands = numpy.delete(bands, 4, 2)

    # then run the cloud probability function with np.newaxis to bring reshaped_bands from 3D to 4D array which is what the function needs
    logger.info(f'Setting up Cloud Detector with the parameters; threshold: {threshold}, average_over: {average_over}, dilation_size: {dilation_size}, all_bands: {all_bands}')
    cloud_detector = S2PixelCloudDetector(threshold=threshold, average_over=average_over, dilation_size=dilation_size, all_bands=all_bands)

    logger.info(f'Running cloud detector for {basename} ...')
    cloud_mask = cloud_detector.get_cloud_masks(bands[numpy.newaxis, ...])
    # cloud_prob_map = cloud_detector.get_cloud_probability_maps(refl_reshaped_bands[np.newaxis, ...])

    # rasterio now complains about the 4D shape so squeeze it back down
    reshaped_cloud_mask = numpy.squeeze(cloud_mask)
    # reshaped_cloud_prob_map = np.squeeze(cloud_prob_map)

    # need to re-instate the no_data values ('0') in the cloud mask
    # identify non-zero values in original input (just check first band). This returns a boolean array, which is then converted to integer
    # with binary values of 0 (no_data) and 1 (valid cells). This is then multiplied with the cloud mask to re-insert no_data values
    zero_mask = bands[:,:,0]  != 0
    nodata_mask = zero_mask.astype(int)
    cloud_mask_nodata = reshaped_cloud_mask * nodata_mask

    # set the metadata for binary cloud mask
    profile = meta
    profile.update(
        dtype=rasterio.uint8,
        count=1
    )   

    # write cloud mask output
    with rasterio.open(outputFile, 'w', **profile) as dst:
        dst.write_band(1, cloud_mask_nodata.astype(rasterio.uint8))

    return outputFile

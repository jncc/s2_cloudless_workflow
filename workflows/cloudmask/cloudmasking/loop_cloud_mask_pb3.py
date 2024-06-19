# Pauline Burke 041223
# This Python script loops through .tif files in a folder, and feeds each into the S2PixelCloudDetector to produce a cloud mask from
# Level 1C input (these are stacked 10m 13 band .tifs derived from the original Level 1C ESA individual bands). Outputs a 10m cloud mask

from s2cloudless import S2PixelCloudDetector
import os
import time
import rasterio
import numpy as np

# set up input and output directories
inDir = "/home/ubuntu/cloud_masking/s2cloudless/outputs/Pre-250122"
outDir = "/home/ubuntu/cloud_masking/s2cloudless/cloud_masks"

directory = os.fsencode(inDir)

# initialise cloud detector, setting desired threshold and other parameters. N.B. The 'average_over' and 'dilation_size' are generally fine as-is
# the threshold value however can be modified - this relates to cloud probability, as derived in the first step of the cloud detector. For instance
# a 'threshold=0.6' will output a cloud mask for pixels determined to have a 60% or greater probability of being cloudy. 'threshold=0.4' would 
# output cloud mask of 40% probability or greater. Testing has generally shown a value of 'threshold=0.6' to produce most useful results and avoid
# false positives in areas such as beaches or high reflectivity targets in e.g. urban areas or fields.
cloud_detector = S2PixelCloudDetector(threshold=0.6, average_over=4, dilation_size=2, all_bands=False)

# loop over files in the input directory
for file in os.listdir(directory):
    filename = os.fsdecode(file)
    # only iterate over files with *.tif extension
    if filename.endswith(".tif"):
        file_temp = os.path.splitext(filename)
        file_stripped = file_temp[0]
        # set output filename and path location
        outFile = str(file_stripped + '_cloud.tif')
        outPath = os.path.join(outDir, outFile)
        print(str('Reading ' + file_stripped + '...'))
        
        # open and read the input .tif file, storing in a dataset. Copy the .tif metadata
        read_time_start = time.time()
        with rasterio.open(filename) as ds:
            bands = ds.read()
            meta = ds.meta.copy()

        # bands shape: (10, 12066, 12066)
        # required shape needs the band count (10) at the end so juggle the axis around so it looks like: (12066, 12066, 10)
        # for verification, print out the shape of the inputs before and after
        print(np.shape(bands))
        reshaped_bands = np.moveaxis(bands, [0, 1, 2], [2, 0, 1])
        print(np.shape(reshaped_bands))

        # remove the unnecessary bands (B03, B06, B07) as the cloud detector for some unkknown reason does not work when all 13 bands are 
        # passed and flag set to True
        mod_bands = np.delete(reshaped_bands, 2, 2)
        mod_bands_2 = np.delete(mod_bands, 4, 2)
        mod_bands_3 = np.delete(mod_bands_2, 4, 2)
        ten_bands = mod_bands_3
        # verify shape of input dataset by prinitng to screen
        print(np.shape(ten_bands))

        # cloud detector requires units as REFLECTANCE. Convert DN to Reflectance as 'Refl = DN / 1000'
        refl_reshaped_bands = ten_bands / 10000

        # calculate and print time taken to read and prep the input data
        read_time_end = time.time()
        read_time_total = (read_time_end - read_time_start)
        final_read_time = (read_time_total / 60)
        print(str('Time to read and prepare input file: ' + str(final_read_time) + ' minutes'))

        # then run the cloud probability function with np.newaxis to bring reshaped_bands from 3D to 4D array which is what the function needs
        print(str('Running cloud detector for ' + file_stripped + '...'))
        masking_time_start = time.time()
        cloud_mask = cloud_detector.get_cloud_masks(refl_reshaped_bands[np.newaxis, ...])
        # cloud_prob_map = cloud_detector.get_cloud_probability_maps(refl_reshaped_bands[np.newaxis, ...])
        masking_time_end = time.time()

        # calculate and print time taken to run cloud masking
        masking_time_total = (masking_time_end - masking_time_start)
        final_masking_time = (masking_time_total / 60)
        print(str('Time to run cloud masking for ' + file_stripped + ': ' + str(final_masking_time) + ' minutes'))
        

        # rasterio now complains about the 4D shape so squeeze it back down
        write_start = time.time()
        reshaped_cloud_mask = np.squeeze(cloud_mask)
        # reshaped_cloud_prob_map = np.squeeze(cloud_prob_map)

        # need to re-instate the no_data values ('0') in the cloud mask
        # identify non-zero values in original input (just check first band). This returns a boolean array, which is then converted to integer
        # with binary values of 0 (no_data) and 1 (valid cells). This is then multiplied with the cloud mask to re-insert no_data values
        zero_mask = ten_bands[:,:,0]  != 0
        nodata_mask = zero_mask.astype(int)
        cloud_mask_nodata = reshaped_cloud_mask * nodata_mask

        # set the metadata for binary cloud mask
        profile = meta
        profile.update(
            dtype=rasterio.uint8,
            count=1
        )   

        # set the metadata for cloud probability map
        # write cloud mask output
        with rasterio.open(outPath, 'w', **profile) as dst:
            dst.write_band(1, cloud_mask_nodata.astype(rasterio.uint8))
        
        # output time taken to reshape and write out the cloud mask file
        write_end = time.time()
        write_time = (write_end - write_start)
        final_write_time = (write_time / 60)
        print(str('Time to write output for ' + file_stripped + ': ' + str(final_write_time) + ' minutes'))

        del bands, meta, ds, reshaped_bands, mod_bands, mod_bands_2, mod_bands_3, ten_bands, refl_reshaped_bands, cloud_mask, reshaped_cloud_mask, zero_mask, nodata_mask, cloud_mask_nodata, profile, dst, read_time_start, read_time_end, read_time_total, final_read_time, masking_time_start, masking_time_end, masking_time_total, final_masking_time, write_start, write_end, write_time, final_write_time 

    else:
        continue

#!/bin/bash

# dirs="S2A_MSIL1C_20210124T112351_N0500_R037_T30UVG_20230527T134148.SAFE"
inDir="/home/ubuntu/cloud_masking/s2cloudless/inputs"
outDir="/home/ubuntu/cloud_masking/s2cloudless/outputs"

for dirs in */ ; do
	
	echo "$dirs"
	filename=$(awk '{print substr($0, 39, 22)}' <<< "$dirs")
	outFile="$outDir/$filename"
	cd $dirs/GRANULE/L1C*/IMG_DATA/
	gdalbuildvrt -resolution highest -srcnodata 0 -vrtnodata 0 -r bilinear -separate $outFile.vrt *_B01.jp2 *_B02.jp2 *_B03.jp2 *_B04.jp2 *_B05.jp2 *_B06.jp2 *_B07.jp2 *_B08.jp2 *_B8A.jp2 *_B09.jp2 *_B10.jp2 *_B11.jp2 *_B12.jp2
	gdal_translate -ot UInt16 -a_nodata 0 -co COMPRESS=DEFLATE $outFile.vrt $outFile.tif
	# mv $filename.vrt $filename.tif $outDir
	# gdalwarp -t_srs EPSG:27700 -tr 10 10 -ot UInt16 -r bilinear -srcnodata 0 -dstnodata 0 -co COMPRESS=DEFLATE -co BIGTIFF=YES $outFile.vrt $outFile.tif
	cd $inDir
done

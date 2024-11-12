# ARD cloud masks script

Finds the cloud mask outputs from the workflow and copies/symlinks them to new files with Sentinel-2 ARD names based on the matching ARD files it can find on the file system. Intended to be run on JASMIN with CEDA access.

python ard_cloudmasks.py --startdate 2020-01-01 --enddate 2020-01-31 --inputdir /path/to/s2cloudless/cloudmasks --arddir /path/to/ard --outputdir /path/to/renamed/cloudmasks --symlink true --reportfile /optional/path/to/report/file.csv

Notes:
    * Should work with both the older and newer S2 ARD names
    * Should work with multiple cloud masks for different ESA product baselines (it will use the latest version)
    * Should handle split granules by ordering by processing time or the "SPLIT1" names to do the matching
# `cloudmask` workflow

This is a luigi workflow that takes in a single Sentinel 2 SAFE directory and runs the S2 Cloudless package over the data to generate a cloudmask, this is then passed into fMask to generate a cloud shadow mask, these masks are merged into a single output (1 = cloud / 2 = cloud shadow) and then optionally reprojected. 

There is an example config file that contains some default parameters, based around using the working directory (`/working/data`). Can be run with the following from the workflows directory.

```bash
PYTHONPATH='.' LUIGI_CONFIG_PATH='/working/data/luigi.cfg' luigi --module cloudmask CleanupTemporaryFiles --inputPath=/working/data/S2A_MSIL1C_20240505T110621_N0510_R137_T30UXD_20240505T131002.SAFE --local-scheduler
```

Assuming you have a downloaded and unzipped SAFE folder to feed it should run through to generating a cloud mask using s2 cloudless and then use that to try and generate a cloud mask using python FMask. The full command can be as follows;

```bash
PYTHONPATH='.' luigi --module cloudmask CleanupTemporaryFiles
    --inputPath=${PATH_TO_SAFE_DIR_OR_SAFE_ZIP}
    --stateFolder=${PATH_TO_STATE_FOLDER}
    --workingFolder=${PATH_TO_WORKING_FOLDER}
    --outputFolder=${PATH_TO_OUTPUT_FOLDER}
    --cloudDetectorThreshold=0.6
    --cloudDetectorAverageOver=4
    --cloudDetectorDilationSize=2
    --cloudDetectorAllBands=False
    --bufferData=True
    --bufferDistance=100
    --reproject=True
    --reprojectionEPSG=27700
    --keepIntermediates
    --keepInputFiles
    --local-scheduler
```

The `cloudDetector` arguments are the current default parameters and allow us to tweak the S2 Cloudless process, used in the `GenerateCloudmask` step.

```bash
    --cloudDetectorThreshold=0.6
    --cloudDetectorAverageOver=4
    --cloudDetectorDilationSize=2
    --cloudDetectorAllBands=False
```

The `reproject` arguments control if the final output is reprojected and what EPSG code to reproject the data into, and are used in the `ReprojectFiles` step.

```bash
    --reproject=True
    --reprojectionEPSG=27700
```

The `keep` arguments control if the temporary working folder is cleared out after processing, setting the `keepIntermediates` flag to `True` (default: `False`) will clear the temporary working files directory including the following named files; 

- `anglesFile` - Sun angle and azimuth layer used in `GenerateCloudShadowMask` step
- `stackedTOA` - Stacked Top of Atmosphere layer generated directectly from the in put SAFE directory, used to generate `stackedTOARef` intermediate file and used in `GenerateCloudShadowMask` step
- `stackedTOARef` - Stacked Top of Atmosphere layer, converted to DN according to the input SAFE directory metadata (based on Product Baseline) used in the `GenerateCloudMask` step
- `cloudMask` - Interim Cloud Mask layer which is joined with the Cloud Shadow Mask layer
- `cloudShadowMask` - Interim Cloud Shadow Mask layer which is joined with the Cloud Mask layer
- `buffered` - Dictionary containing links to the buffered raster outputs
    - `cloud` - Path to the buffered cloud mask
    - `shadow` - Path to the buffered cloud shadow mask
- `combinedCloudAndShadowMask` - Path to the combined shadow and cloud mask (cloud shadow is put into the file first so is underneath cloud mask)

By default the workflow will delete the file or folder specified by the `inputPath` parameter, to retain this file use `keepInputFile` to prevent this.

## Worfklow Tasks

- CheckInputs -> Check input arguments and folders (**Currently Empty Task**)
- PrepareInputs -> Does some preparatory work on the input SAFE directory
    - Stacks bands into a single image
    - Generates Solar and Satellite Azimuth and Zenith Angles output for help with detecting shadows
    - Generates a stacked image converted into Digital Numbers (DN) using the SAFE metadata to determine the calculaion
- GenerateCloudmask -> Generates a cloud mask using S2 cloudless algorithm
- GenerateCloudShadowMask -> Generates a cloud shadow mask using the cloudmask generated by the previous step in Python FMask
- BufferMasks -> Buffers the cloud and cloud shadow masks by polygonizing the data and buffering that output by a given input (default 100 georeferenced units, should be meters, but controlled by the input projection). During this step the cloud shadow mask is changed to be of value 2 for later use.
- MergeOutputMasks -> Merges the cloud and cloud shadow masks (shadow and then cloud so that cloud is on top), if buffering is disabled then merge the unbuffered outputs
- ReprojectFiles -> Optionally reprojects the mask to a given projection (default: EPSG:27700), only accepts EPSG codes at present
- RunQualityChecks -> Runs a set of quality checks on the output files generated (**Currently Empty Task**)
- GenerateMatada -> Generates a metadata file for the output masks (**Currently Empty Task**)
- CleanupTemporaryFiles -> Removes files from the temporary processing directory (can optionally keep known temporary working files and any loose files in the temporary working directory)

## Requirements

- Python 3.12 (potentially will work on earlier versions but have tested with this version)
- GDAL 3.9 + GDAL Python bindings (requires this to work with Numpy V2, will potentially work with versions after this though)
- SCIPy 1.13.1
- Rasterio 1.3.10
- Python FMask 0.5.9
- S2 Cloudless 1.7.2

Recommend that usage is done either via a conda environment or via a Docker container specified at the root of this project
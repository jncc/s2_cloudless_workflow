# s2_cloudless_workflow

## Container

There is a container currently built off the `fmask.dockerfile` that currently sets up the basics, including a `/working` folder. There is a `/working/data` subfolder under that which should be mounted onto from the the outside to allow for transfer of data into and out of the container. Currently to run the container we are just dropping into bash and running it interactively;

```bash
sudo docker run --rm -it -v ~/local/working/folder:/working/data --entrypoint bash cloudmasking:0.0.1
```

To build to an apptainer container locally:

    docker build -t jncc/cloudmasking .
    docker save jncc/cloudmasking -o cloudmasking.tar
    apptainer build cloudmasking.sif docker-archive://cloudmasking.tar

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
    --tempFolder=${PATH_TO_TEMP_FOLDER}
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
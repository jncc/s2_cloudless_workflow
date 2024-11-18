#!/bin/bash
#SBATCH -p short-serial
#SBATCH --time-min=01:00
#SBATCH --time=0-2:00:00
#SBATCH --mem=18G
#SBATCH -D $jobWorkingDir
#SBATCH -o $jobWorkingDir/%J.out
#SBATCH -e $jobWorkingDir/%J.err
##SBATCH -A defra_eo_jncc_s2_cloudmask

/apps/singularity/bin/singularity exec --bind $workingMount:/work --bind $stateMount:/state --bind $inputMount:/input --bind $outputMount:/output $s2CloudmaskContainer PYTHONPATH='/working/software/workflows' luigi --module cloudmask CleanupTemporaryFiles --inputPath=$inputPath --stateFolder=/state --tempFolder=/work --outputFolder=/output bufferData=$bufferData --bufferDistance=$bufferDistance --reproject=$reproject --reprojectionEPSG=$reprojectionEPSG --local-scheduler
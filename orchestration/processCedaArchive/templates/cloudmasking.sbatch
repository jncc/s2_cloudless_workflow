#!/bin/bash
#SBATCH -p short-serial
#SBATCH --time=02:00:00
#SBATCH --mem=18G
#SBATCH -D $stateMount
#SBATCH -o $stateMount/%J.out
#SBATCH -e $stateMount/%J.err
##SBATCH -A defra_eo_jncc_s2_cloudmask

/usr/bin/singularity exec --cleanenv $dataMounts --bind $workingMount:/work --bind $stateMount:/state --bind $inputMount:/input --bind $outputMount:/output $s2CloudmaskContainer /working/software/workflows/cloudmask/container/exec.sh --module cloudmask CleanupTemporaryFiles --inputPath=$inputPath --stateFolder=/state --tempFolder=/work --outputFolder=/output $buffer $reproject --local-scheduler
$cleanupWorkingFolders
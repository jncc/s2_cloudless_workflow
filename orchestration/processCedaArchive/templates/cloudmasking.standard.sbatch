#!/bin/bash
#SBATCH --job-name="$jobName"
#SBATCH --time=02:00:00
#SBATCH --mem=18G
#SBATCH --account=$account
#SBATCH --partition=standard
#SBATCH --qos=standard
#SBATCH -D $stateMount
#SBATCH -o $stateMount/%J.out
#SBATCH -e $stateMount/%J.err

/usr/bin/singularity exec --cleanenv $dataMounts --bind $workingMount:/work --bind $stateMount:/state --bind $inputMount:/input --bind $outputMount:/output $s2CloudmaskContainer /working/software/workflows/cloudmask/container/exec.sh --module cloudmask $luigiTarget --inputPath=$inputPath --stateFolder=/state --tempFolder=/work --outputFolder=/output $buffer $reproject --local-scheduler

if [ -f $endingStatefilePath ]; then
    rm -r $workingMount
fi
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

/usr/bin/singularity exec --cleanenv $dataMounts --bind $workingMount:/working/data/temp --bind $stateMount:/working/data/state --bind $inputMount:/working/data/input --bind $outputMount:/working/data/output $s2CloudmaskContainer /working/software/workflows/cloudmask/container/exec.sh --module cloudmask $luigiTarget --inputPath=/working/data/input/$inputPath --stateFolder=/working/data/state --tempFolder=/working/data/temp --outputFolder=/working/data/output $buffer $reproject --local-scheduler

if [ -f $endingStatefilePath ]; then
    rm -r $workingMount
fi
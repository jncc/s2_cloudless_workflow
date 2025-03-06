#!/bin/bash
#SBATCH --job-name="$jobName"
#SBATCH --time=02:00:00
#SBATCH --cpus-per-task=2
##SBATCH --mem=32G
#SBATCH --account=$account
#SBATCH --partition=highres
#SBATCH --qos=highres
#SBATCH -D $stateMount
#SBATCH -o $stateMount/%J.out
#SBATCH -e $stateMount/%J.err

/usr/bin/singularity exec --cleanenv $dataMounts --bind $workingMount:/working/data/work --bind $tmpMount:/tmp --bind $stateMount:/working/data/state --bind $inputMount:/working/data/input --bind $outputMount:/working/data/output $s2CloudmaskContainer /working/software/workflows/cloudmask/container/exec.sh --module cloudmask $luigiTarget --inputPath=/working/data/input/$inputPath --stateFolder=/working/data/state --workingFolder=/working/data/work --outputFolder=/working/data/output $buffer $reproject --local-scheduler

if [ -f $endingStatefilePath ]; then
    rm -r $workingMount
fi
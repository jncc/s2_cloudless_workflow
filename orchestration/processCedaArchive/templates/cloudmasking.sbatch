#!/bin/bash
#SBATCH --job-name "s2-cloudmasking"
#SBATCH --partition standard
#SBATCH --qos high
#SBATCH --cpus-per-task=2
#SBATCH --account $account
#SBATCH --time 04:00:00
#SBATCH --mem 64G
#SBATCH -D $workspaceFolder
#SBATCH -o $workspaceFolder/%J.out
#SBATCH -e $workspaceFolder/%J.err

/usr/bin/singularity exec --cleanenv $dataMounts --bind $workingMount:/work --bind $tmpMount:/tmp --bind $stateMount:/state --bind $inputMount:/input --bind $outputMount:/output $s2CloudmaskContainer /working/software/workflows/cloudmask/container/exec.sh --module cloudmask $luigiTarget --inputPath=/input/$inputPath --stateFolder=/state --workingFolder=/work --outputFolder=/output $buffer $reproject --keepIntermediates --keepInputFiles --local-scheduler

if [ -f $endingStatefilePath ]; then
    rm -r $workingMount
    rm $inputMount/$inputPath
fi

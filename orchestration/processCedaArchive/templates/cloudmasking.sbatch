# Run workflow serially
#!/bin/bash
#SBATCH -p short-serial
#SBATCH --time-min=01:00
#SBATCH --time=0-2:00:00
#SBATCH --mem=18G
#SBATCH -D $jobWorkingDir
#SBATCH -o $jobWorkingDir/%J.out
#SBATCH -e $jobWorkingDir/%J.err
##SBATCH -A defra_eo_jncc_s2_cloudless

/apps/singularity/bin/singularity exec --bind $workingMount:/working --bind $stateMount:/state --bind $inputMount:/input --bind $outputMount:/output $s2CloudmaskContainer /app/exec.sh CleanupTemporaryFiles --productName=$productName $removeSourceFileFlag
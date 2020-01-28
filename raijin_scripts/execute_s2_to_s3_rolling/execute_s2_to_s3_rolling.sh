#!/usr/bin/env bash

# Set up PBS job variables
#PBS -P v10
#PBS -q copyq

#PBS -l wd

## Resource limits
#PBS -l mem=1GB
#PBS -l ncpus=1
#PBS -l walltime=10:00:00

## The requested job scratch space.
#PBS -l jobfs=1GB
#PBS -l storage=gdata/if87+gdata/v10
#PBS -m abe -M nci.monitor@dea.ga.gov.au

# echo on and exit on fail
set -ex

# Set up our environment
# shellcheck source=/dev/null
source "$HOME"/.bashrc

# Load the latest stable DEA module
module use /g/data/v10/public/modules/modulefiles
module load dea

python3 s2_to_s3_rolling.py "${NUM_DAYS}" "${S3_BUCKET}" "${END_DATE}" "${UPDATE}"

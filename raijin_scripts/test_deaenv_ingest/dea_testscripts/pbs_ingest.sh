#!/bin/bash

## Project name
#PBS -P v10

## Queue type
#PBS -q express

## The total memory limit across all nodes for the job
#PBS -l mem=32GB

## The requested job scratch space.
#PBS -l jobfs=1GB

## The number of cpus required for the job to run.
#PBS -l ncpus=16
#PBS -l walltime=20:00:00

## The job will be executed from current working directory instead of home.
## PBS -l wd

#PBS -N Ingest

## Export all environment vairables in the qsub command environment to be exported to the
## batch job
#PBS -V

echo "
  ------------------------------------------------------
   -n 'Job is running on node '; cat $PBS_NODEFILE
  ------------------------------------------------------
   PBS: qsub is running on $PBS_O_HOST
   PBS: Originating queue      = $PBS_O_QUEUE
   PBS: Executing queue        = $PBS_QUEUE
   PBS: Working directory      = $PBS_O_WORKDIR
   PBS: Execution mode         = $PBS_ENVIRONMENT
   PBS: Job identifier         = $PBS_JOBID
   PBS: Job name               = $PBS_JOBNAME
   PBS: Node_file              = $PBS_NODEFILE
   PBS: Current home directory = $PBS_O_HOME
   PBS: PATH                   = $PBS_O_PATH
  ------------------------------------------------------"
echo ""
module use /g/data/v10/public/modules/modulefiles
module load "${MODULE}"

# Check if we can connect to the database
datacube -vv -C "$CONFIG_FILE" system check

echo
echo "======================================================================"
echo "| Indexing Landsat 8 $STR_NAME 25 metre, 100km tile                   "
echo "| Australian Albers Equal Area projection (EPSG:3577)                 "
echo "======================================================================"
echo
cd "$OUTDIR"/ingest_configfiles/ || exit 0
datacube -vv -C "$CONFIG_FILE" ingest -c "$YAML_FILE"

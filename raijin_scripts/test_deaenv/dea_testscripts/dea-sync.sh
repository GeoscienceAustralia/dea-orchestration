#!/usr/bin/env bash
## Project name
#PBS -P u46

## Queue type
#PBS -q express

## The total memory limit across all nodes for the job
#PBS -l mem=32GB

## The requested job scratch space.
#PBS -l jobfs=1GB
#PBS -lother=gdata1:gdata2

## The number of cpus required for the job to run
#PBS -l ncpus=16
#PBS -l walltime=10:00:00

#PBS -N qsub_sync

shopt -s globstar
SYNCDIR="$WORKDIR"/work/sync
TRASH_ARC=no

##########################################
###      PBS job information.          ###
##########################################
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

# shellcheck source=/dev/null
source "$TESTDIR"/dea_testscripts/setup_deamodule_env.sh "$MUT" "$DATACUBE_CONFIG_PATH"

dc_version=$(datacube --version)

echo "********************************************************************"
echo "  Datacube Config Path (sync):  $DATACUBE_CONFIG_PATH"
echo "  Datacube version under test:  $dc_version"
echo "  PATH (sync):  $PATH"
echo "********************************************************************"
echo ""

# Check if we can connect to the database
datacube -vv system check

echo "Starting dea-submit-sync process......"
##################################################################################################
# Run dea-sync process
##################################################################################################
# Read agdc datasets from the database before Sync process
echo ""
echo "**********************************************************************"
echo "Read previous agdc_dataset product names and count before Sync process"
psql -h agdcdev-db.nci.org.au -p 6432 -d "$DBNAME" -c 'select name, count(*) FROM agdc.dataset a, agdc.dataset_type b where a.dataset_type_ref = b.id group by b.name'
echo "**********************************************************************"
echo ""

TRASH_ARCHIVED=''
if [ "$TRASH_ARC" == yes ]; then
   TRASH_ARCHIVED='--trash-archived'
fi

cd "$SYNCDIR" || exit 0
CACHE_DIR="$SYNCDIR/cache_$RANDOM"
mkdir -p "$CACHE_DIR"
qsub -V -W block=true -N dea-sync -q express -W umask=33 -l wd,walltime=10:00:00,mem=25GB,ncpus=1 -m ae -M santosh.mohan@ga.gov.au -P u46 -- dea-sync -vvv --cache-folder "$CACHE_DIR" -j 4 --log-queries "$TRASH_ARCHIVED" --update-locations --index-missing "$PATH_TO_PROCESS"

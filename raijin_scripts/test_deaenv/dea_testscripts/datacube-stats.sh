#!/bin/bash
## Project name
#PBS -P u46

## Queue type
#PBS -q normal

## The total memory limit across all nodes for the job
#PBS -l mem=32GB

## The requested job scratch space.
#PBS -l jobfs=1GB

## The number of cpus required for the job to run
#PBS -l ncpus=16
#PBS -l walltime=10:00:00

#PBS -M santosh.mohan@ga.gov.au
#PBS -m ae

#PBS -N dc-stats

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

# shellcheck source=/dev/null
source "$TESTDIR"/dea_testscripts/setup_deamodule_env.sh "$MUT" "$DATACUBE_CONFIG_PATH"
module load otps

# Load PCM module for testing new geomedian statistics
module use /g/data/u46/users/ia1511/public/modules/modulefiles/
module load pcm

stats_version=$(datacube-stats --version)

echo "********************************************************************"
echo "  Datacube Config Path (stats):  $DATACUBE_CONFIG_PATH"
echo "  Datacube stats version under test:  $stats_version"
echo "  PATH (stats):  $PATH" 
echo "********************************************************************"
echo ""

# Check if we can connect to the database
datacube -vv system check

echo "Read previous agdc_dataset product names and count before fractional cover process"
psql -h agdcdev-db.nci.org.au -p 6432 -d "$DBNAME" -c 'select name, count(*) FROM agdc.dataset a, agdc.dataset_type b where a.dataset_type_ref = b.id group by b.name'
echo "**********************************************************************"

datacube-stats --help

echo ""
echo "List of available statistics......"
datacube-stats -C "$CONFIGFILE" -E datacube -vvv --list-statistics

echo ""
echo "Starting datacube stats on $PRODUCT......"
datacube-stats -C "$CONFIGFILE" -E datacube -vvv --year 2018 --log-file "$WORKDIR"/work/stats/"$PRODUCT".log --log-queries "$WORKDIR"/stats_configfiles/"$PRODUCT".yaml

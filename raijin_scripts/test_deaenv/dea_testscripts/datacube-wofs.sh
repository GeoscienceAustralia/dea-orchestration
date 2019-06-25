#!/usr/bin/env bash
## Project name
#PBS -P u46

## Queue type
#PBS -q normal

## The total memory limit across all nodes for the job
#PBS -l mem=62GB

## The requested job scratch space.
#PBS -l jobfs=1GB
#PBS -lother=gdata1:gdata2

## The number of cpus required for the job to run
#PBS -l ncpus=16
#PBS -l walltime=10:00:00

#PBS -N testWOfS

shopt -s globstar

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

wofs_version=$(datacube-wofs --version)

echo "********************************************************************"
echo "  Datacube Config Path (WOfS):  $DATACUBE_CONFIG_PATH" 
echo "  Datacube WOfS version under test:  $wofs_version"
echo "  PATH (WOfS):  $PATH" 
echo "********************************************************************"
echo ""

# Check if we can connect to the database
datacube -vv system check

echo "Starting datacube-wofs process......"
# This is required as WOfS code expects user to configure work root
# else work root shall be defaulted to '/g/data/v10/work/' folder
export DEA_WORK_ROOT=$WORKDIR/work/wofs
##################################################################################################
# Submit a WOfS job to Raijin
##################################################################################################
SUBMISSION_LOG="$WORKDIR"/work/wofs/wofs-$(date '+%F-%T').log
cd "$WORKDIR/work/wofs" || exit 0

# Read agdc datasets from the database before Wofs process
{
echo "
********************************************************************
   Datacube Config Path (WOfS):  $DATACUBE_CONFIG_PATH
   DEA WORK ROOT (WOfS):  $DEA_WORK_ROOT
********************************************************************

Read previous agdc_dataset product names and count before WOfS process"
psql -h agdcdev-db.nci.org.au -p 6432 -d "$DBNAME" -c 'select name, count(*) FROM agdc.dataset a, agdc.dataset_type b where a.dataset_type_ref = b.id group by b.name'
echo "**********************************************************************"
datacube-wofs list
datacube-wofs ensure-products --app-config "$WORKDIR"/wofs_configfiles/wofs_albers.yaml -C "$CONFIGFILE" -vvv --dry-run
datacube-wofs ensure-products --app-config "$WORKDIR"/wofs_configfiles/wofs_albers.yaml -C "$CONFIGFILE" -vvv

datacube-wofs submit --app-config "$WORKDIR"/wofs_configfiles/wofs_albers.yaml -M santosh.mohan@ga.gov.au -m ae -P u46 -q normal -C "$CONFIGFILE" -vvv --year "$YEAR" --tag "$YEAR" --no-qsub
datacube-wofs submit --app-config "$WORKDIR"/wofs_configfiles/wofs_albers.yaml -M santosh.mohan@ga.gov.au -m ae -P u46 -q normal -C "$CONFIGFILE" -vvv --year "$YEAR" --tag "$YEAR" --dry-run
datacube-wofs submit --app-config "$WORKDIR"/wofs_configfiles/wofs_albers.yaml -M santosh.mohan@ga.gov.au -m ae -P u46 -q normal -C "$CONFIGFILE" -vvv --year "$YEAR" --tag "$YEAR"

sleep 5s  

} > "$SUBMISSION_LOG"

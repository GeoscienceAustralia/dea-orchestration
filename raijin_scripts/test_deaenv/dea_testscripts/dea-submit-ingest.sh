#!/usr/bin/env bash
## Project name
#PBS -P u46

## Queue type
#PBS -q express

## The total memory limit across all nodes for the job
#PBS -l mem=32GB

## The requested job scratch space.
#PBS -l jobfs=1GB

## The number of cpus required for the job to run
#PBS -l ncpus=16
#PBS -l walltime=10:00:00

#PBS -N qsub_ingest

shopt -s globstar
INGESTDIR="$WORKDIR"/work/ingest
JOB_NAME="Ingest_${PRODUCT}"

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
echo "  Datacube Config Path (ingest):  $DATACUBE_CONFIG_PATH" 
echo "  Datacube version under test:  $dc_version"
echo "  PATH (ingest):  $PATH" 
echo "********************************************************************"
echo ""

# Check if we can connect to the database
datacube -vv system check

echo "Starting dea-submit-ingest process......"
echo "
===================================================================
| Ingest Landsat 8 Surface Reflectance NBAR 25 metre, 100km tile  |
| Australian Albers Equal Area projection (EPSG:3577)             |
==================================================================="
echo ""
##################################################################################################
# Run dea-submit-ingest process
##################################################################################################
# Read agdc datasets from the database before Ingest process
echo ""
echo "**********************************************************************"
echo "Read previous agdc_dataset product names and count before Ingest process"
psql -h agdcdev-db.nci.org.au -p 6432 -d "$DBNAME" -c 'select name, count(*) FROM agdc.dataset a, agdc.dataset_type b where a.dataset_type_ref = b.id group by b.name'
echo "**********************************************************************"
echo ""

cd "$INGESTDIR" || exit 0
yes 2>/dev/null | dea-submit-ingest qsub --project u46 --queue express -n 5 -t 10 -m a -M santosh.mohan@ga.gov.au -W umask=33 --name "$JOB_NAME" -c "$WORKDIR"/ingest_configfiles/"${PRODUCT}".yaml -C "$CONFIGFILE" --allow-product-changes "${PRODUCT}" "${YEAR}"

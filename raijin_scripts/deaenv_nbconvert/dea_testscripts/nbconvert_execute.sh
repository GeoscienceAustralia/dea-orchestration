#!/usr/bin/env bash
## Project name
#PBS -P u46

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

## Paths for outputs and Error files
#PBS -e /g/data/v10/work/dea_env_test/log_files
#PBS -o /g/data/v10/work/dea_env_test/log_files

#PBS -N NBConvert_Test

## Export all environment vairables in the qsub command environment to be exported to the
## batch job
#PBS -V

WORKDIR=/g/data/v10/work/dea_env_test
NBFILE="$TEST_BASE"/dea_testscripts/requirements_met.ipynb
OUTPUTDIR="$WORKDIR"/output_files/nbconvert/requirements_met-"$(date '+%Y-%m-%d')".html

# Load DEA module
# shellcheck source=/dev/null
echo Loading module "${MODULE}"
module use /g/data/v10/public/modules/modulefiles
module load "${MODULE}"

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

##################################################################################################
## Run a test notebook convert using PBS
##################################################################################################
## Convert a notebook to an python script and print the stdout
## To remove code cells from the output, use templateExporter
jupyter nbconvert --to python "$NBFILE" --stdout --TemplateExporter.exclude_markdown=True

## Execute the notebook
## Cell execution timeout = 5000s, --ExecutePreprocessor.timeout=5000
## --allow-errors shall allow conversion will continue and the output from
## any exception be included in the cell output
jupyter nbconvert --ExecutePreprocessor.timeout=5000 --to notebook --execute "$NBFILE" --allow-errors
mv -f "$TEST_BASE"/dea_testscripts/requirements_met.nbconvert.ipynb "$WORKDIR"/output_files/nbconvert

## Finally convert using notebook to html file
jupyter nbconvert --to html "$NBFILE" --stdout > "$OUTPUTDIR"

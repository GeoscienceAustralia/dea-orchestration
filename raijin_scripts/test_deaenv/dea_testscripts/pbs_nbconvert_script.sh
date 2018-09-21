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
#PBS -l walltime=05:00:00

#PBS -N Test_NBConvert

##########################################
###      PBS job information.          ###
##########################################
##########################################
SUBMISSION_LOG="$WORKDIR"/work/nbconvert/nbconvert-$(date '+%F-%T').log

echo "" > "$SUBMISSION_LOG"
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
  ------------------------------------------------------" >> "$SUBMISSION_LOG"
echo "" >> "$SUBMISSION_LOG"

# Run a Notebook convert on the requirements met notebook with the module under test
NBFILE="$WORKDIR"/work/nbconvert/requirements_met.ipynb

# Copy requirements_met notebook file to work directory so that we can update 
# the latest dea module in the notebook file
cp "$TESTDIR"/dea_testscripts/requirements_met.ipynb "$NBFILE"

# Update the module under test
sed -i -e 's,DEA_MUT,'"$MUT"',' "$NBFILE"

OUTPUTDIR="$WORKDIR"/work/nbconvert/requirements_met-"$(date '+%Y-%m-%d')".html
cd "$WORKDIR" || exit 0

# Load DEA module
# shellcheck source=/dev/null
source "$TESTDIR"/dea_testscripts/setup_deamodule_env.sh "$MUT" "$TESTDIR/$DC_CONF"

## Convert a notebook to an python script and print the stdout
## To remove code cells from the output, use templateExporter
jupyter nbconvert --to python "$NBFILE" --stdout --TemplateExporter.exclude_markdown=True

## Execute the notebook
## Cell execution timeout = 5000s, --ExecutePreprocessor.timeout=5000
## --allow-errors shall allow conversion will continue and the output from 
## any exception be included in the cell output
jupyter nbconvert --ExecutePreprocessor.timeout=5000 --to notebook --execute "$NBFILE" --allow-errors
[ -f "$TESTDIR"/dea_testscripts/requirements_met.nbconvert.ipynb ] && mv -f "$TESTDIR"/dea_testscripts/requirements_met.nbconvert.ipynb "$WORKDIR"/work/nbconvert/requirements_met.nbconvert.ipynb

## Finally convert using notebook to html file
jupyter nbconvert --to html "$NBFILE" --stdout > "$OUTPUTDIR"

## Remove temp file
[ -f "$TESTDIR"/dea_testscripts/mydask.png ] && rm -f "$TESTDIR"/dea_testscripts/mydask.png
[ -f "$WORKDIR"/work/nbconvert/mydask.png ] && rm -f "$WORKDIR"/work/nbconvert/mydask.png

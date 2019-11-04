#!/bin/bash
##!/usr/bin/python
#PBS -l wd,walltime=10:00:00,mem=4GB,jobfs=1GB
#PBS -P v10
#PBS -q copyq
#PBS -W umask=33
#PBS -m abe -M nci.monitor@dea.ga.gov.au

# shellcheck disable=SC2154,SC2206
# Skip SC2154, var is referenced but not assigned.
# Skip SC2206, Quote to prevent word splitting/globbing, or split robustly with mapfile or read -a.
# shellcheck source=/dev/null
source "$HOME"/.bashrc
module use /g/data/v10/public/modules/modulefiles

# Module wagl/5.4.0 requires dea-env/20190329
module load dea/20190329

set -x  # echo on
export AWS_PROFILE="${profile}"
aws configure list

#  module use /g/data/v10/public/modules/modulefiles/;
#
ret_val=$(module use /g/data/v10/private/modules/modulefiles/; module load wagl/5.4.0;
python3 "${root_dir}"/s3_sync.py "${file_name}")

IFS=', ][' read -r -a PATHS_TO_PROCESS <<< "$ret_val"

for syncpath in "${PATHS_TO_PROCESS[@]}"
do
  ###########################################################################################################
  # Recursively sync all the files under a specified directory to S3 bucket excluding specified file formats
  ###########################################################################################################
  cd "$syncpath" || exit 1
  dirname=$(basename "${syncpath}")
  dtime=$(basename "$(dirname "${syncpath}")")

  # Sync everything except NBART and LAMBERTIAN (if available)
  time aws s3 sync --quiet "$syncpath" "$s3bucket/$dtime/$dirname" --exclude "NBART/*"

  tempdir="/g/data/v10/tmp/temp/$(uuidgen)"

  mkdir -p "$tempdir"

  # Script to modify ARD-METADATA.yaml and save the changes to the new file in a separate directory on the NCI,
  python "$root_dir/update_metadata.py" "$syncpath/ARD-METADATA.yaml" "$tempdir"

  # Copy the updated ARD-METADATA.yaml file to s3
  aws s3 cp "$tempdir/ARD-METADATA.yaml" "$s3bucket/$dtime/$dirname/ARD-METADATA.yaml"

  # Remove temp directory once new ARD-METADATA.yaml file is uploaded to s3
  rm -R "$tempdir"

done

# Archive old data (retain only 2 years of data) once we have successfully processed new data
ARCHIVE_START_DATE=$(date '+%Y-%m-%d' -d "-737 days")  # (365*2 + 7) days
ARCHIVE_END_DATE=$(date '+%Y-%m-%d' -d "-730 days")  # 365*2 days

while [[ "$ARCHIVE_START_DATE" < "$ARCHIVE_END_DATE" ]]
do
    dt=$(date -d"$ARCHIVE_START_DATE +1 days" +"%Y-%m-%d")
    echo "Remove data from S3 bucket $s3bucket/$dt and archive the same from database"
    set -x

    # Store job ids in an array variable
    aws s3 rm "$s3bucket/$dt"

    # Archive from database
    datacube dataset search product=ga_s2a_ard_nbar_granule time=${dt} | grep "^id: " | cut -d ":" -f2 | \
    xargs datacube dataset archive
    datacube dataset search product=ga_s2b_ard_nbar_granule time=${dt} | grep "^id: " | cut -d ":" -f2 | xargs \
    xargs datacube dataset archive

    set +x

    ARCHIVE_START_DATE="$dt"

done

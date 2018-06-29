#!/usr/bin/env bash

SYNC_LOGS=/g/data/u46/users/sm9911/work/dea-sync_pbs_logs/
SYNC_CACHE=/g/data/u46/users/sm9911/work/dea-sync_pbs_logs/cachedir/
DEA_MODULE=dea/20180515
QUEUE=express
PROJECT=v10

echo Loading module "${DEA_MODULE}"
echo Submitting PBS job to run dea-sync -vvv --cache-folder "${SYNC_CACHE}"

module use /g/data/v10/public/modules/modulefiles
module use /g/data/v10/private/modules/modulefiles

module load "${DEA_MODULE}"

mkdir -p "${SYNC_CACHE}"
cd "${SYNC_LOGS}" || exit 0

qsub -V -N dea-sync -q "${QUEUE}" -W umask=33 -l wd,walltime=5:00:00,mem=25GB,ncpus=1 -P "${PROJECT}" -- dea-sync -vvv --cache-folder "${SYNC_CACHE}"

#!/bin/bash

# In order for the double-asterisk glob to work, the globstar option needs to be set
# To disable globstar: shopt -u globstar
shopt -s globstar

# Telemetry
cd /g/data/v10/repackaged/rawdata/0/2016 || exit 0
datacube -vv -C "$1" dataset add --auto-match ./**/**/ga-metadata.yaml
cd /g/data/v10/repackaged/rawdata/0/2017 || exit 0
datacube -vv -C "$1" dataset add --auto-match ./**/**/ga-metadata.yaml
cd /g/data/v10/repackaged/rawdata/0/2018 || exit 0
datacube -vv -C "$1" dataset add --auto-match ./**/**/ga-metadata.yaml

# Level1 Scenes
cd /g/data/v10/reprocess/ls7/level1/2016 || exit 0
datacube -vv -C "$1" dataset add --auto-match ./**/**/ga-metadata.yaml
cd /g/data/v10/reprocess/ls7/level1/2017 || exit 0
datacube -vv -C "$1" dataset add --auto-match ./**/**/ga-metadata.yaml
cd /g/data/v10/reprocess/ls7/level1/2018 || exit 0
datacube -vv -C "$1" dataset add --auto-match ./**/**/ga-metadata.yaml

cd /g/data/v10/reprocess/ls8/level1/2016 || exit 0
datacube -vv -C "$1" dataset add --auto-match ./**/**/ga-metadata.yaml
cd /g/data/v10/reprocess/ls8/level1/2017 || exit 0
datacube -vv -C "$1" dataset add --auto-match ./**/**/ga-metadata.yaml
cd /g/data/v10/reprocess/ls8/level1/2018 || exit 0
datacube -vv -C "$1" dataset add --auto-match ./**/**/ga-metadata.yaml

# NBAR Scenes
cd /g/data/u46/users/dra547/dea_odc_testing || exit 0
datacube -vv -C "$1" dataset add --auto-match ./**/ga-metadata.yaml

# Run a test ingest on NCI PC
echo
echo "======================================================================"
echo "| Indexing Landsat 7/8 Surface Reflectance NBAR 25 metre, 100km tile |"
echo "| Australian Albers Equal Area projection (EPSG:3577)                |"
echo "======================================================================"
echo
cd "$2"/ingest_configfiles/ || exit 0
datacube -vv -C "$1" ingest -c ls7_nbar_albers.yaml
datacube -vv -C "$1" ingest -c ls8_nbar_albers.yaml

echo
echo "======================================================================="
echo "| Indexing Landsat 7/8 Surface Reflectance NBART 25 metre, 100km tile |"
echo "| Australian Albers Equal Area projection (EPSG:3577)                 |"
echo "======================================================================="
echo
datacube -vv -C "$1" ingest -c ls7_nbart_albers.yaml
datacube -vv -C "$1" ingest -c ls8_nbart_albers.yaml

echo
echo "======================================================================"
echo "| Indexing Landsat 7/8 Pixel Quality 25 metre, 100km tile            |"
echo "| Australian Albers Equal Area projection (EPSG:3577)                |"
echo "======================================================================"
echo
datacube -vv -C "$1" ingest -c ls7_pq_albers.yaml
datacube -vv -C "$1" ingest -c ls8_pq_albers.yaml
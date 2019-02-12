#!/usr/bin/env bash

WORKDIR="$1"
TESTDIR="$2"

# Create new empty directory
mkdir -p "$WORKDIR"/ingest_configfiles
mkdir -p "$WORKDIR"/stats_configfiles
mkdir -p "$WORKDIR"/fc_configfiles
mkdir -p "$WORKDIR"/wofs_configfiles

## Declare datacube array of yaml files to download
declare -a dc_yaml_array=("ls8_nbart_albers.yaml"
                          "ls8_nbar_albers.yaml"
                          "ls8_pq_albers.yaml")
                          
## Declare fractiona cover array of yaml files to download
declare -a fc_yaml_array=("ls8_fc_albers.yaml")

## Declare WOfS array of yaml files to download
declare -a wofs_yaml_array=("wofs_albers.yaml")

## Declare datacube stats yaml file to download
declare -a stats_yaml_array=("item_10"
                             "nbar_ls8_2018_simple_normalised_difference"
                             "nbar_ls8_2018_geomedian"
                             "nbar_ls8_2018_new_geomedian"
                             "nbar_ls8_2018_spectral_mad"
                             "nbar_ls8_2018_tcwbg"
                             "nbar_ls8_2018_tcwbg_shapefile"
                             "nbar_ls8_2018_wet_geomedian_shapefile"
                             "nbar_ls8_2018_simple"
                             "nbar_ls8_2018_simple_shapefile"
                             "pq_count_albers_all_masked_multi_count"
                             "pq_count_albers_annual_shapefile"
                             "fc_ls8_2018_medoid"
                             "fc_ls8_2018_medoid_shapefile"
                             "fc_ls8_2018_medoid_simple"
                             "fc_ls8_2018_medoid_simple_shapefile"
                             "fc_percentile_albers"
                             "fc_percentile_albers_shapefile"
                             "wofsstats"
                             "wofsstats_shapefile"
                             "fc_ls8_2018_medoid_no_prov"
                             "fc_ls8_2018_medoid_no_prov_shapefile"
                             "fc_ls8_2018_none"
                             "fc_ls8_2018_none_shapefile"
                             "fc_ls8_2018_percentile_no_prov"
                             "fc_ls8_2018_percentile_no_prov_shapefile")

# Replace NBAR/NBART/PQ product output location in the yaml file
cd "$WORKDIR"/ingest_configfiles || exit 0
for i in "${dc_yaml_array[@]}"
do
  INGEST_CONF_DIR="https://github.com/geoscienceAustralia/digitalearthau/raw/develop/digitalearthau/config/ingestion/$i"
  yaml_filename=$(basename "$INGEST_CONF_DIR")
  
  wget -q "$INGEST_CONF_DIR"
  sed -i -e 's,location: .*,location: "'"$WORKDIR"'/work/ingest/001",' "$yaml_filename"
done

# Replace fractional cover product output location in the yaml file
cd "$WORKDIR"/fc_configfiles || exit 0
for i in "${fc_yaml_array[@]}"
do
  FC_CONF_DIR="https://github.com/GeoscienceAustralia/fc/raw/master/config/$i"
  yaml_filename=$(basename "$FC_CONF_DIR")
  
  wget -q "$FC_CONF_DIR"
  sed -i -e 's,location: .*,location: "'"$WORKDIR"'/work/fc/001",' "$yaml_filename"
done

# Replace WOfS product output location in the yaml file
cd "$WORKDIR"/wofs_configfiles || exit 0
for i in "${wofs_yaml_array[@]}"
do
  WOfS_CONF_DIR="https://github.com/GeoscienceAustralia/wofs/raw/master/config/$i"
  yaml_filename=$(basename "$WOfS_CONF_DIR")
  
  wget -q "$WOfS_CONF_DIR"
  sed -i -e 's,location: .*,location: "'"$WORKDIR"'/work/wofs/001",' "$yaml_filename"
done

# Replace stats product output location in the yaml file
cd "$WORKDIR"/stats_configfiles || exit 0
for i in "${stats_yaml_array[@]}"
do

  STATS_CONF_FILE="$TESTDIR/dea_testscripts/stats_config_files/$i.yaml"
  cp "$STATS_CONF_FILE" "$WORKDIR"/stats_configfiles
  yaml_file="$WORKDIR/stats_configfiles/$i.yaml"

  sed -i -e 's,location: .*,location: "'"$WORKDIR"'/work/stats/001",' "$yaml_file"
  sed -i -e 's,  start_date: .*,  start_date: 2018-01-01,' "$yaml_file"
  sed -i -e 's,  end_date: .*,  end_date: 2019-01-01,' "$yaml_file"
  sed -i -e 's,  from_file: .*,  from_file: "'"$WORKDIR"'/stats_configfiles/example.shp",' "$yaml_file"
done
cp "$TESTDIR/dea_testscripts/stats_config_files/example.shp" "$WORKDIR"/stats_configfiles
cp "$TESTDIR/dea_testscripts/stats_config_files/example.dbf" "$WORKDIR"/stats_configfiles
cp "$TESTDIR/dea_testscripts/stats_config_files/example.shx" "$WORKDIR"/stats_configfiles
cp "$TESTDIR/dea_testscripts/stats_config_files/example.prj" "$WORKDIR"/stats_configfiles
cp "$TESTDIR/dea_testscripts/stats_config_files/example.qpj" "$WORKDIR"/stats_configfiles

FC_ALBERS="https://github.com/geoscienceAustralia/digitalearthau/raw/develop/digitalearthau/config/products/fc_albers.yaml"
WOFS_ALBERS="https://github.com/geoscienceAustralia/digitalearthau/raw/develop/digitalearthau/config/products/wofs_albers.yaml"
cd "$WORKDIR" || exit 0
wget -q "$FC_ALBERS"
wget -q "$WOFS_ALBERS"

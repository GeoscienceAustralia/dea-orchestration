#!/bin/bash

WORKDIR="$1"

# Remove previous stored config files
[ -d "$WORKDIR/stats_configfiles" ] && rm -r "$WORKDIR/stats_configfiles"

# Create new empty directory
mkdir -p "$WORKDIR"/stats_configfiles

## Declare datacube stats array of yaml files to download
declare -a fcstats_yaml_array=("fc_stats_annual.yaml"
                               "fc_percentile_albers_seasonal.yaml"
                               "fc_percentile_albers_annual.yaml")

## Declare datacube stats array of yaml files to download
declare -a fcpercentiltstats_yaml_array=("fc_percentile_albers.yaml")

declare -a nbarstats_yaml_array=("nbar_stats.yaml")

# Replace FC stats product output location in the yaml file
cd "$WORKDIR"/stats_configfiles || exit 0
for i in "${fcstats_yaml_array[@]}"
do
  FC_STATS_CONF_DIR="https://github.com/GeoscienceAustralia/datacube-stats/raw/master/configurations/fc/$i"
  yaml_filename=$(basename "$FC_STATS_CONF_DIR")

  wget -q "$FC_STATS_CONF_DIR"
  sed -i -e 's,location: .*,location: "'"$WORKDIR"'/fc_stats/001",' "$yaml_filename"
  sed -i -e 's,2018-01-01,2019-01-01,' "$yaml_filename"
  sed -i -e 's,  start_date: .*,  start_date: 2018-01-01,' "$yaml_filename"
  sed -i -e 's,  end_date: .*,  end_date: 2019-01-01,' "$yaml_filename"
  sed -i -e 's,2017-01-01,2019-01-01,' "$yaml_filename"
done

for i in "${fcpercentiltstats_yaml_array[@]}"
do
  FC_STATS_CONF_DIR="https://github.com/GeoscienceAustralia/datacube-stats/raw/master/configurations/fc/$i"
  yaml_filename=$(basename "$FC_STATS_CONF_DIR")

  wget -q "$FC_STATS_CONF_DIR"
  sed -i -e 's,location: .*,location: "'"$WORKDIR"'/fc_stats/001",' "$yaml_filename"
  sed -i -e 's,2018-01-01,2019-01-01,' "$yaml_filename"
  sed -i -e 's,  start_date: .*,  start_date: 2018-01-01,' "$yaml_filename"
  sed -i -e 's,  end_date: .*,  end_date: 2019-01-01\n  stats_duration: 3m\n  step_size: 3m,' "$yaml_filename"
  sed -i -e 's,2017-01-01,2019-01-01,' "$yaml_filename"
done

# Replace NBAR stats product output location in the yaml file
cd "$WORKDIR"/stats_configfiles || exit 0
for i in "${nbarstats_yaml_array[@]}"
do
  NBAR_STATS_CONF_DIR="https://github.com/GeoscienceAustralia/datacube-stats/raw/master/configurations/nbar/$i"
  yaml_filename=$(basename "$NBAR_STATS_CONF_DIR")

  wget -q "$NBAR_STATS_CONF_DIR"
  sed -i -e 's,location: .*,location: "'"$WORKDIR"'/nbar_stats/001",' "$yaml_filename"
  sed -i -e 's,  start_date: .*,  start_date: 2018-01-01,' "$yaml_filename"
  sed -i -e 's,  end_date: .*,  end_date: 2019-01-01,' "$yaml_filename"
done

cp "$2"/dea_testscripts/landsat_seasonal_mean.yaml "$WORKDIR"/stats_configfiles
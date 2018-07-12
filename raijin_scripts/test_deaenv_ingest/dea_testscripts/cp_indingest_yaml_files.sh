#!/bin/bash

WORKDIR="$1"

# Remove previous stored config files
[ -d "$WORKDIR/ingest_configfiles" ] && rm -r "$WORKDIR/ingest_configfiles"

# Create new empty directory
mkdir -p "$WORKDIR"/ingest_configfiles

## Declare datacube array of yaml files to download
declare -a dc_yaml_array=("ls8_nbart_albers.yaml"
                          "ls8_nbar_albers.yaml"
                          "ls8_pq_albers.yaml"
                          "ls7_nbart_albers.yaml"
                          "ls7_nbar_albers.yaml"
                          "ls7_pq_albers.yaml")

# Replace NBAR/NBART/PQ product output location in the yaml file
cd "$WORKDIR"/ingest_configfiles || exit 0
for i in "${dc_yaml_array[@]}"
do
  INGEST_CONF_DIR="https://github.com/opendatacube/datacube-core/raw/develop/docs/config_samples/ingester/$i"
  yaml_filename=$(basename "$INGEST_CONF_DIR")

  wget -q "$INGEST_CONF_DIR"
  sed -e 's,location: .*,location: "'"$WORKDIR"'/work",' "$yaml_filename" > "$yaml_filename.tmp" && mv "$yaml_filename.tmp" "$yaml_filename"
  sed -e 's,description:,metadata_type: eo'"\\n\\"'description:,'  "$yaml_filename" > "$yaml_filename.tmp" && mv "$yaml_filename.tmp" "$yaml_filename"
done
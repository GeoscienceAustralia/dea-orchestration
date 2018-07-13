#!/bin/bash

WORKDIR="$1"

# Remove previous stored config files
[ -d "$WORKDIR/fc_configfiles" ] && rm -r "$WORKDIR/fc_configfiles"

# Create new empty directory
mkdir -p "$WORKDIR"/fc_configfiles

## Declare fractiona cover array of yaml files to download
declare -a fc_yaml_array=("ls5_fc_albers.yaml"
                          "ls7_fc_albers.yaml"
                          "ls8_fc_albers.yaml")

# Replace fractional cover product output location in the yaml file
cd "$WORKDIR"/fc_configfiles || exit 0
for i in "${fc_yaml_array[@]}"
do
  FC_CONF_DIR="https://github.com/GeoscienceAustralia/fc/raw/master/config/$i"
  yaml_filename=$(basename "$FC_CONF_DIR")

  wget -q "$FC_CONF_DIR"
  sed -e 's,location: .*,location: "'"$WORKDIR"'/work",' "$yaml_filename" > "$yaml_filename.tmp" && mv "$yaml_filename.tmp" "$yaml_filename"
done
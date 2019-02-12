#!/bin/bash

# Migrate from production database
module use /g/data/v10/public/modules/modulefiles/

set -x  # Turn on Echo
module unload "$MUT"

module load dea/20180515
set +x  # Turn off Echo

export DATACUBE_CONFIG_PATH="$CONFIGFILE"
dea-test-env check
dea-test-env teardown
dea-test-env setup
datacube -E datacube system check

# Time range is picked from cubedash dashboard such that datasets for 
# input_region tile [10, -40] is processed during this period in the production 
# database. This input_region tile is used in test configuration files as query
# argument.
echo "Migrate ls8_nbart_scene product from production database to test database"
dea-test-env migrate -S production --product ls8_nbart_scene '2018-08-06 < time < 2018-08-08'  

echo "Migrate ls8_nbar_scene product from production database to test database"
dea-test-env migrate -S production --product ls8_nbar_scene '2018-08-06 < time < 2018-08-08'

echo "Migrate ls8_pq_scene product from production database to test database"
dea-test-env migrate -S production --product ls8_pq_scene '2018-08-06 < time < 2018-08-08'

echo "Migrate ls8_pq_legacy_scene product from production database to test database"
dea-test-env migrate -S production --product ls8_pq_legacy_scene '2018-08-06 < time < 2018-08-08'

echo "Migrate ls8_nbar_albers product from production database to test database"
dea-test-env migrate -S production --product ls8_nbar_albers '2018-08-14 < time < 2018-08-16'
dea-test-env migrate -S production --product ls8_nbar_albers '2018-08-21 < time < 2018-08-23'

echo "Migrate ls8_pq_albers product from production database to test database"
dea-test-env migrate -S production --product ls8_pq_albers '2018-08-14 < time < 2018-08-16'
dea-test-env migrate -S production --product ls8_pq_albers '2018-08-21 < time < 2018-08-23'

echo "Migrate dsm1sv10 product (required for WOfS) from production database to test database"

set -x  # Turn on Echo
module unload dea/20180515
sleep 5s # Wait for 5 seconds

module load "$MUT"

set +x  # Turn off Echo

# Update dsm1sv10 product definition in the test database with the one we want as per the production database 
datacube -C "$CONFIGFILE" -E NCI-test product update "$TEMP_DIR/dsm.yaml" --allow-unsafe
datacube -C "$CONFIGFILE" -E NCI-test dataset add \
/g/data/v10/eoancillarydata/elevation/dsm1sv1_0_Clean_tiff/agdc-metadata.yaml --confirm-ignore-lineage -p dsm1sv10

# Update fc and wofs albers product definition to the test database
echo "Add FC and WOFS product definition to test database"
datacube -C "$CONFIGFILE" -E NCI-test product add "$TEMP_DIR/fc_albers.yaml"
datacube -C "$CONFIGFILE" -E NCI-test product add "$TEMP_DIR/wofs_albers.yaml"

echo ""
echo "Show dsm1sv10 product definition from the test database"
datacube -C "$CONFIGFILE" -E NCI-test product show dsm1sv10

echo ""
echo ""
echo "Show dsm1sv10 product definition from the production database"
datacube -C "$CONFIGFILE" -E production product show dsm1sv10
echo ""

echo "Remove temp files:"
rm "$TEMP_DIR/wofs_albers.yaml"
rm "$TEMP_DIR/fc_albers.yaml"
rm "$TEMP_DIR/dsm.yaml"

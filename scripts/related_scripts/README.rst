###############
RELATED SCRIPTS
###############

This folder contains a collection of helper scripts related to DEA's orchestration infrastructure.

 * :bash:`restore_odc_db_from_nci.sh`: Restores the database from a pgdump hosted on NCI infrastructure
   * Usage :bash:`restore_odc_db_from_nci.sh {{YOUR.pgdump}}`
   * Relies on the PGHOST, PGUSER, PGDATABASE variables being set; assumes port 5432

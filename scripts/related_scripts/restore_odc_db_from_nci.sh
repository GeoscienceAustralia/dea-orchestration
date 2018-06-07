#!/bin/bash

set -e

RESTORE_FILE="$1"
RESTORE_PROCESSES=2

SCRIPT_RESTORE_OPTIONS=( "-O" "--exit-on-error" "--no-tablespaces" "--no-acl" "--verbose" "--schema=agdc" )

PRIMARY_TABLES_IDENTIFIER=" metadata_type"
TERTIARY_TABLES_IDENTIFIER='\( dataset_location \| dataset_source \)'

RESTORE_REFERENCE="./$(date +%s)_pgrestore_script"
LOG_FILE="$RESTORE_REFERENCE.log"

check_args() {
    # Checks to see if the environment variables are set
    if [[ -z "$RESTORE_FILE" ]]; then
        echo "Please provide the restore file as an argument to this script"
        return 1
    fi

    if [[ -z "$PGHOST" ]]; then
        echo "Please set PGHOST to the database host you want to restore to"
        return 1
    fi

    if [[ -z "$PGUSER" ]]; then
        echo "Please set PGUSER to the database user you want to restore with"
        return 1
    fi

    if [[ -z "$PGDATABASE" ]]; then
        echo "Please set the PGDATABASE to the database in the dumpfile"
        return 1
    fi

    return 0
}

# Add additional tables to the search_path
alter_search_path() {
    # shellcheck disable=SC2016
    psql --host="$PGHOST" --username="$PGUSER" -c "ALTER ROLE $PGUSER SET search_path TO "'"$user", agdc, public' postgres
}

# Create SCHEMA
# Clear any old data and restore the schema to the database
create_schema() {
    pg_restore --schema-only -l "$RESTORE_FILE" | grep -v 'EXTENSION' > "$RESTORE_REFERENCE.schema_list.txt"
    pg_restore "${SCRIPT_RESTORE_OPTIONS[@]}" -L "$RESTORE_REFERENCE.schema_list.txt" --dbname=postgres --host="$PGHOST" --username="$PGUSER" --schema-only --create --clean "$RESTORE_FILE" >> "$LOG_FILE"
}

# Apply EXTENSIONS
# Ensure that the extensions are created under the public schema (for pg_dump)
apply_extensions() {
    echo "SET search_path TO 'public';" > "$RESTORE_REFERENCE.extension_list.sql"
    pg_restore -l "$RESTORE_FILE" | grep 'EXTENSION -' | cut -d' ' -f6 | sed 's/\(.*\)/CREATE EXTENSION \1;/' >> "$RESTORE_REFERENCE.extension_list.sql"
    psql --host="$PGHOST" --username="$PGUSER" -f "$RESTORE_REFERENCE.extension_list.sql" postgres >> "$LOG_FILE"
}

# Restore DATA

# Metadatatypes are restored first as records are a dependency for other tables
# grep for the schema agdc allows the script to ignore any tables added to the public space by extensions which come with their own data sets
restore_metadatatypes() {
    pg_restore --data-only -l "$RESTORE_FILE" | grep agdc | grep -e "$PRIMARY_TABLES_IDENTIFIER" > "$RESTORE_REFERENCE.table_list1.txt"
    pg_restore "${SCRIPT_RESTORE_OPTIONS[@]}" -L "$RESTORE_REFERENCE.table_list1.txt" --dbname="$PGDATABASE" --host="$PGHOST" --username="$PGUSER" --data-only --jobs="$RESTORE_PROCESSES" "$RESTORE_FILE" >> "$LOG_FILE"
}

# Dataset table and other non-related tables are restored next
# grep for the schema agdc allows the script to ignore any tables added to the public space by extensions which come with their own data sets
restore_dataset() {
    pg_restore --data-only -l "$RESTORE_FILE" | grep agdc | grep -ve "$TERTIARY_TABLES_IDENTIFIER" | grep -ve "$PRIMARY_TABLES_IDENTIFIER" > "$RESTORE_REFERENCE.table_list2.txt"
    pg_restore "${SCRIPT_RESTORE_OPTIONS[@]}" -L "$RESTORE_REFERENCE.table_list2.txt" --dbname="$PGDATABASE" --host="$PGHOST" --username="$PGUSER" --data-only --jobs="$RESTORE_PROCESSES" "$RESTORE_FILE" >> "$LOG_FILE"
}

# Restores dataset_location and dataset_source which have dependencies on dataset
# grep for the schema agdc allows the script to ignore any tables added to the public space by extensions which come with their own data sets
restore_dataset_dependent_data() {
    pg_restore --data-only -l "$RESTORE_FILE" | grep agdc | grep -e "$TERTIARY_TABLES_IDENTIFIER" > "$RESTORE_REFERENCE.table_list3.txt"
    pg_restore "${SCRIPT_RESTORE_OPTIONS[@]}" -L "$RESTORE_REFERENCE.table_list3.txt" --dbname="$PGDATABASE" --host="$PGHOST" --username="$PGUSER" --data-only --jobs="$RESTORE_PROCESSES" "$RESTORE_FILE" >> "$LOG_FILE"
}

# Run
check_args
alter_search_path
create_schema
apply_extensions
restore_metadatatypes
restore_dataset
restore_dataset_dependent_data

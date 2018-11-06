CREATE SCHEMA deamonitoring;
CREATE EXTENSION file_fdw;
CREATE SERVER fileserver
  FOREIGN DATA WRAPPER file_fdw;

CREATE OR REPLACE FOREIGN TABLE deamonitoring.loadavg
(one float, five float, fifteen float, scheduled text, pid int)
    SERVER fileserver
    OPTIONS (filename '/proc/loadavg', format 'text', delimiter ' ');

CREATE OR REPLACE FOREIGN TABLE deamonitoring.meminfo
(stat text, value text)
    SERVER fileserver
    OPTIONS (filename '/proc/meminfo', format 'csv', delimiter ':');

create function deamonitoring.disk_free_space()
  returns TABLE(filesystem character varying, blocks integer, used integer, available integer, percent_use character, mount_point character varying)
language plpgsql as $$
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS temp_table (
    filesystem  varchar,
    blocks      int,
    used        int,
    available   int,
    percent_use char(5),
    mount_point varchar
  ) on commit drop;
  COPY temp_table FROM PROGRAM 'df -P | awk ''{print $1","$2","$3","$4","$5","$6}''' with (FORMAT csv, HEADER true);
  -- OR df -P | tr -d '%'  | tr -s ' ' ','

  RETURN QUERY
  SELECT * from temp_table;
  DROP TABLE temp_table;
END;
$$;

CREATE OR REPLACE FUNCTION deamonitoring.num_cores()
  RETURNS INT AS $BODY$
BEGIN
  DROP TABLE IF EXISTS ncpus_table;
  CREATE TEMPORARY TABLE ncpus_table (
    num INT
  );
  COPY ncpus_table (num) FROM PROGRAM 'nproc --all';
  RETURN (SELECT num FROM ncpus_table);
END;
$BODY$
LANGUAGE plpgsql;

DROP FUNCTION deamonitoring.net_if_stats();
CREATE OR REPLACE FUNCTION deamonitoring.net_if_stats()
  RETURNS TABLE(dev            varchar, bytes_in bigint, packets_in bigint, errs_in bigint, drop_in bigint, fifo_in bigint,
                frame_in       bigint, compressed_in bigint, multicast_in bigint,
                bytes_out      bigint, packets_out bigint, errs_out bigint, drop_out bigint, fifo_out bigint, colls_out bigint, carrier_out bigint,
                compressed_out bigint)
language plpgsql as $$
BEGIN

  DROP TABLE IF EXISTS ifstats;
  CREATE TEMPORARY TABLE ifstats (
    dev            varchar,
    bytes_in       bigint,
    packets_in     bigint,
    errs_in        bigint,
    drop_in        bigint,
    fifo_in        bigint,
    frame_in       bigint,
    compressed_in  bigint,
    multicast_in   bigint,
    bytes_out      bigint,
    packets_out    bigint,
    errs_out       bigint,
    drop_out       bigint,
    fifo_out       bigint,
    colls_out      bigint,
    carrier_out    bigint,
    compressed_out bigint
  );
  COPY ifstats FROM PROGRAM 'cat /proc/net/dev | tail -n +3 | tr '':|'' ''  '' | awk ''{$2=$2};1''' WITH (DELIMITER ' ');
  RETURN QUERY (SELECT * FROM ifstats);
end;
$$



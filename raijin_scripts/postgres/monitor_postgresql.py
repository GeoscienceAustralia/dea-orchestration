"""

Done:
-- Free Disk Space
-- Network Traffic
-- Load Average

TODO: Check security on functions and tables
TODO: RAM Monitoring
-- Need to monitor
-- CPU Usage
-- RAM Usage
-- I/O Usage
-- https://russ.garrett.co.uk/2015/10/02/postgres-monitoring-cheatsheet/
"""
import copy
from datetime import datetime

import psycopg2
from apscheduler.schedulers.blocking import BlockingScheduler
from elasticsearch import Elasticsearch

ES_INDEX = 'metricbeat-dea-mon-'
ES_HOST = 'search-digitalearthaustralia-lz7w5p3eakto7wrzkmg677yebm.ap-southeast-2.es.amazonaws.com:443'
BEAT_NAME = 'agdc-db.nci.org.au'


def post_to_es(doc):
    es = Elasticsearch(ES_HOST, use_ssl=True)
    now = datetime.utcnow()
    doc = copy.deepcopy(doc)
    doc.update({
        '@timestamp': now.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        'beat': {
            'version': 'dea-mon-0.1',
            'name': BEAT_NAME,
            'hostname': BEAT_NAME,
        }
    })

    print(doc)
    index = ES_INDEX + now.strftime('%Y-%m-%d')
    res = es.index(index=index, doc_type='doc', body=doc)
    print(res)
    es.indices.refresh(index=index)


def loadavg(conn):
    # create a cursor
    with conn.cursor() as cur:
        cur.execute('SELECT * from deamonitoring.loadavg')

        # display the PostgreSQL database server version
        one, five, fifteen, _, _ = cur.fetchone()

        cur.execute('SELECT deamonitoring.num_cpus()')
        cores, = cur.fetchone()

    post_to_es({
        'metricset': {
            'name': 'load',
            'module': 'system',
        },
        'system': {
            'load': {
                '1': one,
                '5': five,
                '15': fifteen,
                'norm': {
                    '1': one / cores,
                    '5': five / cores,
                    '15': fifteen / cores,
                },
                'cores': cores,
            }
        },

    })


def disk_space(conn):
    with conn.cursor() as cur:
        cur.execute('select * from deamonitoring.disk_free_space();')
        fsdetails = cur.fetchall()
        print(cur.fetchall())

    for device, blocks, used, available, percent_use, mount in fsdetails:
        post_to_es({
            'metricset': {
                'module': 'system',
                'name': 'filesystem',
            },
            'system': {
                'filesystem': {
                    'available': available,
                    'device_name': device,
                    'mount_point': mount,
                    'free': available,
                    'total': blocks,
                    'used': {
                        'bytes': used,
                        'pct': (blocks - available) / blocks
                    }
                }
            }
        })


def network(conn):
    with conn.cursor() as cur:
        cur.execute('select * from deamonitoring.net_if_stats()')
        for dev, bi, pi, erri, dropi, _, _, _, _, bo, po, erro, dropo, _, _, _, _ in cur:
            post_to_es({
                'metricset': {
                    'module': 'system',
                    'name': 'network',
                },
                'system': {
                    'network': {
                        'out': {
                            'bytes': bo,
                            'packets': po,
                            'errors': erro,
                            'dropped': dropo
                        },
                        'in': {
                            'bytes': bi,
                            'packets': pi,
                            'errors': erri,
                            'dropped': dropi

                        },
                        'name': dev,
                    }
                }
            })


def meminfo(conn):
    with conn.cursor() as cur:
        cur.execute('SELECT * FROM deamonitoring.meminfo;')
        print(cur.fetchall())


setup_functions = {
    'disk_free_space': """
create function deamonitoring.disk_free_space()
  returns TABLE(filesystem character varying, blocks integer, used integer, available integer, percent_use character, mount_point character varying)
language plpgsql as $$
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS temp_table (filesystem varchar, blocks int, used int, available int, percent_use char(5), mount_point varchar) on commit drop;
  COPY temp_table FROM PROGRAM 'df -P | awk ''{print $1","$2","$3","$4","$5","$6}''' with (FORMAT csv, HEADER true);
  -- OR df -P | tr -d '%'  | tr -s ' ' ','

  RETURN QUERY
    SELECT *
      from temp_table;
  DROP TABLE temp_table;
END;
$$;
    """
}


def main():
    scheduler = BlockingScheduler()

    with psycopg2.connect(host="agdc-db.nci.org.au", database="datacube") as conn:
        scheduler.add_job(loadavg, 'interval', seconds=30, args=[conn])
        scheduler.add_job(network, 'interval', seconds=30, args=[conn])
        scheduler.add_job(disk_space, 'interval', minutes=15, args=[conn])
        scheduler.start()


if __name__ == '__main__':
    main()

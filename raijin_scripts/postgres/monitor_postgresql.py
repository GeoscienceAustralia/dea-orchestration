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
import json
from datetime import datetime

from functools import wraps
import psycopg2
import psycopg2.extras
from apscheduler.schedulers.blocking import BlockingScheduler
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient

ES_INDEX = 'metricbeat-dea-mon-'
ES_HOST = 'search-digitalearthaustralia-lz7w5p3eakto7wrzkmg677yebm.ap-southeast-2.es.amazonaws.com:443'
BEAT_NAME = 'agdc-db.nci.org.au'


def main():
    setup_es_mapping()
    scheduler = BlockingScheduler()

    with psycopg2.connect(host="agdc-db.nci.org.au", database="datacube") as conn:
        scheduler.add_job(loadavg, 'interval', seconds=30, args=[conn])
        scheduler.add_job(network, 'interval', seconds=30, args=[conn])
        scheduler.add_job(mem_usage, 'interval', seconds=30, args=[conn])
        scheduler.add_job(cpu_usage, 'interval', seconds=30, args=[conn])
        scheduler.add_job(disk_space, 'interval', minutes=5, args=[conn])
        scheduler.start()


def pg_retry(func):
    @wraps(func)
    def inner(conn):
        try:
            return func(conn)
        except psycopg2.InternalError:
            conn.abort()
            return func(conn)

    return inner


def setup_es_mapping():
    es = Elasticsearch(ES_HOST, use_ssl=True)
    ic = IndicesClient(es)

    with open('mapping-template.json') as fin:
        mapping_template = json.load(fin)

    res = ic.put_template(name='metricbeat-dea-mon', body=mapping_template)
    print(res)


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


@pg_retry
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


cpu0 = None


@pg_retry
def cpu_usage(conn):
    global cpu0
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute('SELECT deamonitoring.num_cpus()')
        cores, = cur.fetchone()
        cur.execute('select * from deamonitoring.cpu_stats()')
        new_cpu = cur.fetchone()
    if cpu0 is None:
        cpu0 = new_cpu.copy()
    else:
        diff = {stat: new_cpu[stat] - cpu0[stat] for stat in new_cpu.keys() if stat != 'core'}
        cpu0 = new_cpu.copy()
        # total = diff['usr'] + diff['nice'] + diff['system'] + diff['idle'] + diff['iowait'] + diff['irq'] + diff['softirq'] + diff['steal']
        total = sum(diff.values())
        post_to_es({
            "metricset": {
                "module": "system",
                "name": "cpu",
            },
            "system": {
                "cpu": {
                    "cores": cores,
                    "idle": {
                        "pct": diff['idle'] / total,
                    },
                    "iowait": {
                        "pct": diff['iowait'] / total,
                    },
                    "irq": {
                        "pct": diff['irq'] / total,
                    },
                    "nice": {
                        "pct": diff['nice'] / total,
                    },
                    "softirq": {
                        "pct": diff['softirq'] / total,
                    },
                    "steal": {
                        "pct": diff['steal'] / total,
                    },
                    "system": {
                        "pct": diff['system'] / total,
                    },
                    "total": {
                        "pct": (total - diff['idle']) / total,
                    },
                    "user": {
                        "pct": diff['usr'] / total,
                    }
                }
            }
        })


@pg_retry
def mem_usage(conn):
    # See https://access.redhat.com/solutions/406773
    with conn.cursor() as cur:
        cur.execute("select stat, trim(trailing ' kB' from trim(' ' from value))::bigint * 1024 as num from meminfo")
        meminfo = {stat: value
                   for stat, value in cur}

    actual_free = meminfo['MemFree'] + meminfo['Buffers'] + meminfo['Cached']
    post_to_es({
        'metricset': {
            'module': 'system',
            'name': 'memory'
        },
        'system': {
            "memory": {
                "actual": {
                    "free": actual_free,
                    "used": {
                        "bytes": meminfo['MemTotal'] - actual_free,
                        "pct": (meminfo['MemTotal'] - actual_free) / meminfo['MemTotal']
                    }
                },
                "free": meminfo['MemFree'],
                "swap": {
                    "free": meminfo['SwapFree'],
                    "total": meminfo['SwapTotal'],
                    "used": {
                        "bytes": meminfo['SwapTotal'] - meminfo['SwapFree'],
                        "pct": 0 if meminfo['SwapTotal'] == 0 else (meminfo['SwapTotal'] - meminfo['SwapFree']) /
                                                                   meminfo['SwapTotal']
                    }
                },
                "total": meminfo['MemTotal'],
                "used": {
                    "bytes": meminfo['MemTotal'] - meminfo['MemFree'],
                    "pct": (meminfo['MemTotal'] - meminfo['MemFree']) / meminfo['MemTotal']
                }
            }
        }
    })


@pg_retry
def disk_space(conn):
    with conn.cursor() as cur:
        cur.execute('select * from deamonitoring.disk_free_space()')
        fsdetails = cur.fetchall()
        print(cur.fetchall())

    fscount = 0
    total_free = 0
    total_used = 0
    for device, blocks, used, available, percent_use, mount in fsdetails:
        fscount += 1
        total_free += available
        total_used += used
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
    post_to_es({
        'metricset': {
            'module': 'system',
            'name': 'fsstat'
        },
        "system": {
            "fsstat": {
                "count": fscount,
                "total_size": {
                    "free": total_free,
                    "total": total_free + total_used,
                    "used": total_used
                }
            }
        }
    })


@pg_retry
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

if __name__ == '__main__':
    main()

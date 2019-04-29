
NQSTAT_OUTPUT = """
    Job     User Project      %CPU  WallTime  Time Lim       vmem       mem    memlim   cpus

express open&run =============================
2909671 R fxl554  u46   runpbs   6  04:01:45  05:00:00    11.22GB   82.57MB    32.0GB     16
2909684 R fxl554  u46   runpbs   6  04:00:02  05:00:00    10.38GB   82.63MB    32.0GB     16
2909893 R fxl554  u46   runpbs   6  03:55:56  05:00:00    10.74GB   82.62MB    32.0GB     16

       3 running jobs (0 suspended jobs), 0 queued jobs, 48 cpus in use, 0 cpus suspended, 0 cpus queued

normal open&run =============================
2904443 R kk7182  u46   kk-ipy   0  06:39:25  08:00:00     4.28GB    3.11GB     8.0GB      4

       1 running jobs (0 suspended jobs), 0 queued jobs, 4 cpus in use, 0 cpus suspended, 0 cpus queued

copyq open =============================

       0 running jobs (0 suspended jobs), 0 queued jobs, 0 cpus in use, 0 cpus suspended, 0 cpus queued
"""


def test_parse_nqstat():
    lines = NQSTAT_OUTPUT.split('\n')[2:]
    current_queue = None
    jobs = []
    for line in lines:
        if line and line[0] != ' ':
            current_queue = line.split()[0]

        parts = line.split()
        if len(parts) == 12:
            jobs.append({'job': parts[0],
                         'state': parts[1],
                         'user': parts[2],
                         'project': parts[3],
                         'job-name': parts[4],
                         'cpu-percent': parts[5],
                         'walltime': parts[6],
                         'time-limit': parts[7],
                         'vmem': parts[8],
                         'mem': parts[9],
                         'mem-limit': parts[10],
                         'ncpus': parts[11],
                         'queue': current_queue})
    print(jobs)

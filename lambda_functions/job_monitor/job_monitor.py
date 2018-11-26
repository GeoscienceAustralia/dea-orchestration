import re
from datetime import datetime

from elasticsearch import helpers

from dea_es import ES_CONNECTION as ES
from dea_raijin import BaseCommand
from dea_raijin.auth import RaijinSession
from dea_raijin.config import NCI_PROJECTS
from dea_raijin.utils import human2bytes, timestr_to_seconds

GET_USER_CMD = 'getent group {}'
GET_USERJOBS_CMD = 'qstat -wu {}'
GET_JOBINFOEXTRA_CMD = 'qstat -f {}'

# pylint: disable=W0107

class NoJobInfoException(Exception):
    """raised when the application needs to exit early"""

class JobMonitorCommand(BaseCommand):
    COMMAND_NAME = 'JobMonitorCommand'

    def __init__(self):
        super().__init__(self)
        self.raijin = RaijinSession(logger=self.logger)
        self.raijin.connect()

    def command(self, *args, **kwargs):
        users = self._find_users_in_groups(NCI_PROJECTS)
        jobs = self._find_user_jobs(users)
        running_jobs = [job for job in jobs if job['s'] == 'R']
        extra_info = self._find_detailed_job_info([job['job_id'] for job in running_jobs])

        for ei in extra_info:
            for j in jobs:
                if j['job_id'] == ei['job_id']:
                    j.update(ei)
                    break

        t = datetime.utcnow()
        index_name = 'dea-nci-' + t.strftime('%Y-%m-%d')
        es_fields = {
            '_index': index_name,
            '_type': 'job_status',
            '@timestamp': t.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        }

        for j in jobs:
            j.update(es_fields)

        update_template(ES)

        summary = helpers.bulk(client=ES, actions=jobs)
        self.logger.info(summary)

    def _find_users_in_groups(self, *groups):
        stdout, _, exit_code = self.raijin.exec_command(GET_USER_CMD.format(' '.join(*groups)))

        if exit_code:
            self.logger.error('Unable to find users')
            raise RuntimeError('Unable to find users')

        users = set()
        for line in stdout.split('\n'):
            userlist = line.split(':')[-1].split(',')
            users.update(userlist)
        users.discard('')
        return users

    def _find_user_jobs(self, *users):
        header_row_1 = 2
        header_row_2 = 3
        first_data_row = 5

        stdout, _, exit_code = self.raijin.exec_command(GET_USERJOBS_CMD.format(','.join(*users)))

        if exit_code:
            self.logger.error('Unable to find user jobs')
            raise RuntimeError('Unable to find user jobs')

        rows = stdout.split('\n')

        if len(rows) < first_data_row:
            self.logger.info('No jobs found to log')
            raise NoJobInfoException('No user jobs')

        # Get Headers
        headers = self._get_qstat_headers(rows[header_row_1], rows[header_row_2])

        # Create dictionaries
        data = [dict(zip(headers, re.findall(r'([^\s]+)', r))) for r in rows[first_data_row:] if r]

        # process columns
        for r in data:
            try:
                r['reqd_memory'] = human2bytes(r['reqd_memory'].upper())
            except KeyError:
                self.logger.error('Key: reqd_memory not in %s', r.get('job_id', 'job_id_not_found'))
            try:
                r['reqd_time'] = timestr_to_seconds(r['reqd_time'])
            except KeyError:
                self.logger.error('Key: reqd_time not in %s', r.get('job_id', 'job_id_not_found'))
            try:
                r['elap_time'] = timestr_to_seconds(r['elap_time']) if '-' not in r['elap_time'] else None
            except KeyError:
                self.logger.error('Key: elap_time not in %s', r.get('job_id', 'job_id_not_found'))

        return data

    @staticmethod
    def _get_qstat_headers(row_1, row_2):
        headers = re.findall(r'(Job ID|[^\s]+)', row_2)

        start_id = -1  # keep index for next iteration
        for term_id, term in enumerate(headers):
            start_id = row_2.find(term, start_id + 1)
            st_match = re.match(r'([^\s]+)', row_1[start_id:])
            if st_match:
                header_term = st_match.group().strip() + '_' + term
            else:
                header_term = term

            header_term = header_term.lower().replace('\'', '').replace(' ', '_')
            headers[term_id] = header_term

        return headers

    @staticmethod
    def _decode_full_qstat(full_output):
        full_output = full_output.replace('\n\t', '')
        job_outputs = full_output.split('\n\n')

        job_split_outputs = [jo.split('\n') for jo in job_outputs]
        return job_split_outputs

    def _find_detailed_job_info(self, *job_ids):
        stdout, _, _ = self.raijin.exec_command(GET_JOBINFOEXTRA_CMD.format(" ".join(*job_ids)))

        job_extras = [_get_extra_job_fields(j) for j in self._decode_full_qstat(stdout) if j and j[0]]

        for j in job_extras:
            j['resources_used.cput'] = timestr_to_seconds(j.get('resources_used.cput'))
            j['resources_used.walltime'] = timestr_to_seconds(j.get('resources_used.walltime'))

            for k in ['resources_used.cpupercent', 'resources_used.ncpus']:
                try:
                    j[k] = int(j[k])
                except TypeError:
                    j[k] = None
                except KeyError:
                    j[k] = None
                    self.logger.error('Key: %s not in %s', k, j.get('job_id', 'job_id_not_found'))

            try:
                j['cpu_efficiency'] = (j['resources_used.cput'] / j['resources_used.walltime'] /
                                       j['resources_used.ncpus'])
            except (KeyError, TypeError):
                j['cpu_efficiency'] = None
                self.logger.error('Key: cpu_efficiency was unable to be calculated')

        return job_extras


def _get_extra_job_fields(job):
    field_dict = {
        'job_id': job[0].split()[-1]
    }

    for field in job[1:]:
        key, value = field.split(' = ')
        key = key.lower().strip()
        field_dict[key] = value.strip()

    return field_dict


def handler(event, context):
    try:
        return JobMonitorCommand().run()
    except NoJobInfoException:
        pass


def update_template(es):
    jobs_template = {'template': 'dea-nci-*',
                     'mappings': {
                         'job_status': {
                             'properties': {'cpu_efficiency': {'type': 'float'},
                                            'elap_time': {'type': 'float'},
                                            'nds': {'type': 'integer'},
                                            'reqd_memory': {'type': 'long'},
                                            'reqd_time': {'type': 'integer'},
                                            'tsk': {'type': 'integer'},
                                            '@timestamp': {'type': 'date'}}}}}
    es.indices.put_template(name='dea-jobs', body=jobs_template)

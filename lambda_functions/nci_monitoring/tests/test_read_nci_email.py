import json
from pathlib import Path
from unittest.mock import patch, call

import boto3
from moto import mock_s3

from dea_monitoring.read_nci_email import _process_pbs_job_info


@mock_s3
def test_read_nci_emails():
    sample_email = ""

    bucket_name = 'ncimonitoring'
    key = "NCIEmails/01mr4j4vek7i8e6m2v43drie5v79nu1ihfrm8mg1"
    s3 = boto3.resource('s3')
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    bucket = s3.create_bucket(Bucket=bucket_name)

    bucket.upload_file(str(Path(__file__).parent / 'sample_nci_email.txt'), key)

    event_body = {
        'Records': [{
            "s3": {
                "bucket": {"name": bucket_name},
                "object": {"key": key}
            }
        }]
    }
    event = {'Records': [{'body': json.dumps(event_body)}]}


@patch('dea_monitoring.read_nci_email.exec_command')
@patch('dea_monitoring.read_nci_email._push_metadata_to_es')
def test_can_parse_nci_email(push_metadata_mock, exec_command_mock):
    # SETUP to RUN PBS EMAIL JOB PARSING
    # Cannot use .read_text() because it mangles line end characters
    sample_email = (Path(__file__).parent / 'sample_nci_email.txt').read_bytes().decode('utf-8')

    exec_command_mock.return_value = "", "", 0

    # Run the Function
    _process_pbs_job_info(sample_email)

    # Check that the correct lookup command was called via SSH
    assert exec_command_mock.call_args == call(
        'execute_fetch_dataset_info --job-id 7387595.r-man2 --job-name sync_ls7_pq_scene_2019-2019')

    # Check that the correct document was pushed into elasticsearch
    expected_es_doc = call(
        {'pbsjob_raijin': {'info': {'job_id': '7387595.r-man2', 'job_name': 'sync_ls7_pq_scene_2019-2019',
                                    'job_time': 'Fri, 5 Apr 2019 16:38:44 +1100',
                                    'execution_status': 'Execution terminated', 'exit_status': 0},
                           'details': {'job_efficiency (%)': 30.564784053156146,
                                       'memory (MB)': 0.16019058227539062,
                                       'virtual_memory (MB)': 3.0, 'ncpus': 1, 'ndatasets_to_process': 0,
                                       'nfile_creation_pass': 0, 'nfile_creation_fail': 0,
                                       'ndatasets_index_pass': 0,
                                       'ndatasets_index_fail': 0, 'service_units_used': 0.0,
                                       'datasets_processing_efficiency (%)': 0.0}}})

    assert push_metadata_mock.called
    assert push_metadata_mock.call_count == 1
    assert push_metadata_mock.call_args == expected_es_doc

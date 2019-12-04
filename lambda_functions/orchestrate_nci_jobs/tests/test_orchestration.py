from handler import (_extract_after_search_string, _update_job_status_in_dynamodb, datetime, gettz, DATETIME_FORMAT,
                     _add_records_to_dynamodb, _execute_qstat_command, _execute_fetch_jobid_command, _get_job_status,
                     _process_job, _fetch_and_update)


class _FakeDynamoTable:
    def __init__(self):
        self.key = None
        self.update_expr = None
        self.expr_attrs = None

    # pylint: disable=C0103
    def update_item(self, Key, UpdateExpression, ExpressionAttributeValues,
                    *args, **kwargs):
        self.key = Key
        self.update_expr = UpdateExpression
        self.expr_attrs = ExpressionAttributeValues


def test_extract_after_search_string():
    qstat_output = """
    Job Id: 7161209.r-man2
    Job_Name = Test_Job
    job_state = F
    queue = Test_Queue
    qtime = Thu Mar 28 14:00:26 2019
    comment = Job run at Thu Mar 28 at 14:05 on (r720:ncpus=16:mem=33554432kb:j
        obfs_local=104856kb) and failed
    Exit_status = 1
    project = Test_Project
    """
    job_name = _extract_after_search_string("Job_Name = (.*)", qstat_output)
    job_state = _extract_after_search_string("job_state = (.*)", qstat_output)
    project = _extract_after_search_string("project = (.*)", qstat_output)
    queue = _extract_after_search_string("queue = (.*)", qstat_output)
    exit_status = _extract_after_search_string("Exit_status = (.*)", qstat_output)
    comment = _extract_after_search_string("comment = (.*\n.*)", qstat_output)
    queue_time = _extract_after_search_string("qtime = (.*)", qstat_output)

    assert job_name == "Test_Job"
    assert job_state == "F"
    assert project == "Test_Project"
    assert queue == "Test_Queue"
    assert exit_status == "1"
    assert queue_time == "Thu Mar 28 14:00:26 2019"
    assert comment == "Job run at Thu Mar 28 at 14:05 on (r720:ncpus=16:mem=33554432kb:jobfs_local=104856kb) and failed"


def test_add_records_to_dynamodb():
    fake_table = _FakeDynamoTable()
    events_list = {'job_name': 'foo_name',
                   'product': 'foo_product',
                   'project': 'foo_project',
                   'job_queue': 'foo_job_queue',
                   'job_status': 'foo_job_status',
                   'execution_status': 'foo_execution_status',
                   'work_dir': 'foo_work_dir'}

    qsub_job_ids = _add_records_to_dynamodb(fake_table, events_list, "1234567.r-man2,8901234.r-man2,\n")

    assert qsub_job_ids == {"1234567.r-man2", "8901234.r-man2"}
    assert fake_table.update_expr == "SET pbs_job_name = :jname, "\
        "product = :prod, "\
        "job_project = :proj, "\
        "job_queue = :queue, "\
        "job_status = :jstatus, "\
        "execution_status = :estatus, "\
        "queue_timestamp = :tqueue, "\
        "updated_timestamp = :tstamp, "\
        "work_dir = :wdir, "\
        "remarks = :comments"
    assert fake_table.expr_attrs[":jname"] == 'foo_name'
    assert fake_table.expr_attrs[":prod"] == 'foo_product'
    assert fake_table.expr_attrs[":proj"] == 'foo_project'
    assert fake_table.expr_attrs[":queue"] == 'foo_job_queue'
    assert fake_table.expr_attrs[":jstatus"] == 'foo_job_status'
    assert fake_table.expr_attrs[":estatus"] == 'foo_execution_status'
    assert fake_table.expr_attrs[":wdir"] == 'foo_work_dir'
    assert fake_table.expr_attrs[":comments"] == 'NA'


def test_update_job_status_in_dynamodb():
    fake_table = _FakeDynamoTable()
    _update_job_status_in_dynamodb(fake_table, "1234567.foo", "foo_finished", "foo_success")

    assert fake_table.key == {'pbs_job_id': "1234567.foo"}
    assert fake_table.expr_attrs[":jstatus"] == 'foo_finished'
    assert fake_table.expr_attrs[":estatus"] == 'foo_success'


def test_execute_command():
    execs = []

    def mock_qstat_command(*args, **kwargs):
        execs.append('foo execute command')

        return """
            Job Id: 7161209.r-man2
            Job_Name = Test_Job
            job_state = F
            queue = Test_Queue
            qtime = Thu Mar 28 14:00:26 2019
            comment = Job run at Thu Mar 28 at 14:05 on (r720:ncpus=16:mem=33554432kb:j
                obfs_local=104856kb) and failed
            Exit_status = 1
            project = Test_Project""", "", 0

    def mock_exec_command(*args, **kwargs):
        execs.append('foo execute command')

        return "sync_job_state=foo_state\n" \
            "sync_queue_time=Tue Apr 23 12:45:24 2019\n" \
            "sync_exit_status=foo_execution_status\n" \
            "sync_job_name=Test_Sync\n" \
            "sync_project=Test_v10\n" \
            "sync_queue=Test_Normal\n" \
            "sync_comment=Testing\n", \
            "", 0

    output = _execute_qstat_command("1234567.r-man", mock_qstat_command)

    assert len(execs) == 1
    assert execs[0].startswith('foo execute')
    assert output == "_job_name=Test_Job\n" \
        "_job_state=F\n" \
        "_project=Test_Project\n" \
        "_queue=Test_Queue\n" \
        "_exit_status=1\n" \
        "_comment=Job run at Thu Mar 28 at 14:05 on (r720:ncpus=16:mem=33554432kb:jobfs_local=104856kb) and failed\n" \
        "_queue_time=Thu Mar 28 14:00:26 2019\n"

    events_list = {'log_path': '/g/data/foo_name.txt'}
    output = _execute_fetch_jobid_command(events_list, mock_exec_command)

    assert len(execs) == 2
    assert execs[0].startswith('foo execute')
    assert output == "sync_job_state=foo_state\n" \
        "sync_queue_time=Tue Apr 23 12:45:24 2019\n" \
        "sync_exit_status=foo_execution_status\n" \
        "sync_job_name=Test_Sync\n" \
        "sync_project=Test_v10\n" \
        "sync_queue=Test_Normal\n" \
        "sync_comment=Testing\n"


def test_get_job_status():
    # Test task failed scenario
    qsub_job_ids = {"12345.foo_start_id"}
    output = """
    sync_job_state=F
    sync_exit_status=NA
    sync_comment=Testing_Comment
    """
    qsub_job_ids, jobs_failed, job_status, execution_status, comments = _get_job_status(qsub_job_ids,
                                                                                        "1234567.r-man",
                                                                                        False,
                                                                                        output)

    assert job_status == 'FINISHED'
    assert execution_status == "JOB_DELETED"
    assert qsub_job_ids == {"12345.foo_start_id"}
    assert jobs_failed
    assert comments == "Testing_Comment"

    # Test job finished scenario
    qsub_job_ids = {"12345.foo_start_id"}
    output = """
    sync_job_state=F
    sync_exit_status=0
    """
    qsub_job_ids, jobs_failed, job_status, execution_status, comments = _get_job_status(qsub_job_ids,
                                                                                        "1234567.r-man",
                                                                                        False,
                                                                                        output)

    assert job_status == 'FINISHED'
    assert execution_status == "SUCCESS"
    assert qsub_job_ids == {"12345.foo_start_id"}
    assert not jobs_failed
    assert comments == "NA"

    # Test job deleted/suspended scenario
    qsub_job_ids = {"12345.foo_start_id"}
    output = """
    sync_job_state=S
    sync_exit_status=2
    """
    qsub_job_ids, jobs_failed, job_status, execution_status, comments = _get_job_status(qsub_job_ids,
                                                                                        "1234567.r-man",
                                                                                        False,
                                                                                        output)

    assert job_status == 'SUSPENDED'
    assert execution_status == "JOB_SUSPENDED"
    assert qsub_job_ids == {"12345.foo_start_id"}
    assert not jobs_failed
    assert comments == "NA"

    # Test job pending scenario
    qsub_job_ids = {"12345.foo_start_id"}
    output = """
    sync_job_state=R
    sync_exit_status=NA
    """
    qsub_job_ids, jobs_failed, job_status, execution_status, comments = _get_job_status(qsub_job_ids,
                                                                                        "1234567.r-man",
                                                                                        False,
                                                                                        output)

    assert job_status == 'RUNNING'
    assert execution_status == "IN_QUEUE"
    assert qsub_job_ids == {"12345.foo_start_id", "1234567.r-man"}
    assert not jobs_failed
    assert comments == "NA"


def test_process_job():
    execs = []

    def mock_exec_command(*args, **kwargs):
        execs.append('foo execute command')
        job_running = """
            Job_Name = Test_Job1
            job_state = R
            queue = Test_Queue
            qtime = Thu Mar 28 14:00:26 2019
            comment = Job run at Thu Mar 28 at 14:05 on (r720:ncpus=16:mem=33554432kb:j
                obfs_local=104856kb)
            project = Test_Project
        """
        job_finished = """
            Job_Name = Test_Job1
            job_state = F
            queue = Test_Queue
            qtime = Thu Mar 28 14:00:26 2019
            comment = Job run at Thu Mar 28 at 14:05 on (r720:ncpus=16:mem=33554432kb:j
                obfs_local=104856kb) and passed
            Exit_status = 0
            project = Test_Project
        """

        if len(execs) == 2:
            # Return qstat output when _execute_qstat_command function is called
            retval = job_running
        elif len(execs) == 4:
            # Return qstat output when _execute_qstat_command function is called
            retval = job_finished
        else:
            # Return job ids when _execute_fetch_jobid_command function is called
            retval = '1234567.r-man2'

        return retval, "", 0

    input_list = [
        {
            "log_path": "/g/data/foo.log",
            "job_name": "Test_Job",
            "product": "ls8_nbar_scene",
            "project": "Test_Project",
            "job_queue": "Test_Queue",
            "job_status": "IN_QUEUE",
            "execution_status": "IN_QUEUE",
            "work_dir": "/g/data/foo",
            "qsub_job_ids": [
                "8458453.r-man2"
            ]
        },
        {
            "log_path": "/g/data/foo1.log",
            "job_name": "Test_Job1",
            "product": "ls7_nbar_scene",
            "project": "Test_Project",
            "job_queue": "Test_Queue",
            "job_status": "IN_QUEUE",
            "execution_status": "IN_QUEUE",
            "work_dir": "/g/data/foo1",
            "qsub_job_ids": [
                "8458454.r-man2"
            ]
        }
    ]

    fake_table = _FakeDynamoTable()

    pending_jobs, job_failed = _process_job(input_list, fake_table, mock_exec_command)
    assert pending_jobs == {'1234567.r-man2', '8458453.r-man2'}  # "8458454.r-man2" job completed. Do not expect
    assert not job_failed


def test_fetch_and_update():
    execs = []

    def mock_exec_command(*args, **kwargs):
        execs.append('foo execute command')

        return '1234567.r-man2', "", 0

    input_list = [
        {
            "log_path": "/g/data/foo.log",
            "job_name": "sync_foo",
            "product": "ls8_nbar_scene",
            "project": "v10",
            "job_queue": "normal",
            "job_status": "IN_QUEUE",
            "execution_status": "IN_QUEUE",
            "work_dir": "/g/data/foo",
        },
        {
            "log_path": "/g/data/foo1.log",
            "job_name": "sync_foo1",
            "product": "ls7_nbar_scene",
            "project": "v10",
            "job_queue": "normal",
            "job_status": "IN_QUEUE",
            "execution_status": "IN_QUEUE",
            "work_dir": "/g/data/foo1",
        }
    ]

    output_list = [
        {
            "log_path": "/g/data/foo.log",
            "job_name": "sync_foo",
            "product": "ls8_nbar_scene",
            "project": "v10",
            "job_queue": "normal",
            "job_status": "IN_QUEUE",
            "execution_status": "IN_QUEUE",
            "work_dir": "/g/data/foo",
            "qsub_job_ids": [
                "1234567.r-man2"
            ]
        },
        {
            "log_path": "/g/data/foo1.log",
            "job_name": "sync_foo1",
            "product": "ls7_nbar_scene",
            "project": "v10",
            "job_queue": "normal",
            "job_status": "IN_QUEUE",
            "execution_status": "IN_QUEUE",
            "work_dir": "/g/data/foo1",
            "qsub_job_ids": [
                "1234567.r-man2"
            ]
        }
    ]

    fake_table = _FakeDynamoTable()

    event_olist = _fetch_and_update(input_list, fake_table, mock_exec_command)

    assert event_olist == output_list

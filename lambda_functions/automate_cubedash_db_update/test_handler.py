from handler import _update_cubedash_db_config


def test_execute_command():
    execs = []

    def _mock_execute_update_cubedash_config_command(*args, **kwargs):
        execs.append('foo execute command')

        return "", "", 0

    output = _update_cubedash_db_config('test/file',
                                        'test_dbname',
                                        'test_branch',
                                        _mock_execute_update_cubedash_config_command)

    assert len(execs) == 1
    assert execs[0].startswith('foo execute')
    assert not output

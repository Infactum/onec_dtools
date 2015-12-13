import os
import sys
import pytest
import onec_dtools


@pytest.yield_fixture(params=['Platform8Demo/1Cv8.1CD'])
def db_file(request):
    file_path = os.path.join(sys.path[0], 'fixtures', request.param)
    with open(file_path, 'rb') as f:
        yield f


def test_db(db_file):
    db = onec_dtools.Database(db_file)
    for table in db.description:
        for _ in db.read_table(table):
            pass

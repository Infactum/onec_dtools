import os
import sys
import shutil
import pytest
import onec_dtools


@pytest.fixture(params=['Platform8Demo/conf.cf'])
def conf_file(request):
    file_path = os.path.join(sys.path[0], 'fixtures', request.param)
    return file_path


@pytest.yield_fixture(scope='module')
def extract_dir(tmpdir_factory):
    dir = str(tmpdir_factory.mktemp('extract'))
    yield dir
    shutil.rmtree(dir)


@pytest.yield_fixture(scope='module')
def packed_file(tmpdir_factory):
    path = str(tmpdir_factory.getbasetemp().join('packed.cf'))
    yield path
    os.unlink(path)


def test_extract(conf_file, extract_dir):
    onec_dtools.extract(conf_file, extract_dir)


def test_build(extract_dir, packed_file):
    onec_dtools.build(extract_dir, packed_file)
# -*- coding: utf-8 -*-
import os
import sys
import shutil
import pytest
import onec_dtools


@pytest.fixture(params=['Platform8Demo/1cv8.efd'])
def supply_file(request):
    file_path = os.path.join(sys.path[0], 'fixtures', request.param)
    return file_path


@pytest.yield_fixture(scope='module')
def unpack_dir(tmpdir_factory):
    dir = str(tmpdir_factory.mktemp('extract'))
    yield dir
    shutil.rmtree(dir)


def test_unpack(supply_file, unpack_dir):
    with open(supply_file, 'rb') as f:
        onec_dtools.SupplyReader(f).unpack(unpack_dir)


# -*- coding: utf-8 -*-
import os
import sys
import pytest
import onec_dtools


@pytest.yield_fixture(params=['Platform8Demo/1Cv8.1CD'])
def db_file(request):
    file_path = os.path.join(sys.path[0], 'fixtures', request.param)
    with open(file_path, 'rb') as f:
        yield f


def test_parse_whole_db(db_file):
    """
    Дымовой тест полного чтения всех таблиц БД
    """
    db = onec_dtools.DatabaseReader(db_file)
    for table_name, table_desc in db.tables.items():
        for row in db.tables[table_name]:
            for field in table_desc.fields:
                field_value = row[field]
                # Чтение полей неограниченной длины
                if hasattr(field_value, 'value'):
                    _ = field_value.value

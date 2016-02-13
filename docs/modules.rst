Описание модулей
================

database_reader
---------------

.. py:currentmodule:: onec_dtools.database_reader

.. autoclass:: DatabaseReader
    :members:
.. autoclass:: Table
    :members:
    :special-members: __len__, __iter__, __getitem__
.. autoclass:: Row
    :members:
    :special-members: __getitem__
.. autoclass:: Blob
    :members:
    :special-members: __len__, __iter__
.. autoclass:: DBObject
    :members:
    :special-members: __len__
.. autoclass:: FieldDescription
.. autofunction:: database_header
.. autofunction:: root_object
.. autofunction:: raw_tables_descriptions
.. autofunction:: calc_field_size
.. autofunction:: numeric_to_int
.. autofunction:: nvc_to_string
.. autofunction:: bytes_to_datetime

container_reader
----------------
.. py:currentmodule:: onec_dtools.container_reader

.. autoclass:: ContainerReader
    :members:
.. autofunction:: extract
.. autofunction:: read_header
.. autofunction:: read_block
.. autofunction:: read_document
.. autofunction:: read_full_document
.. autofunction:: parse_datetime
.. autofunction:: read_entries


container_writer
----------------
.. py:currentmodule:: onec_dtools.container_writer

.. autoclass:: ContainerWriter
    :members:
    :special-members: __enter__, __exit__
.. autofunction:: build
.. autofunction:: add_entries
.. autofunction:: epoch2int
.. autofunction:: int2hex
.. autofunction:: get_size



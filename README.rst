===============
OneC Data Tools
===============

.. image:: https://img.shields.io/pypi/v/onec_dtools.svg
    :target: https://pypi.python.org/pypi/onec_dtools
.. image:: https://img.shields.io/pypi/pyversions/onec_dtools.svg
    :target: https://pypi.python.org/pypi/onec_dtools
.. image:: https://img.shields.io/pypi/l/onec_dtools.svg
    :target: https://pypi.python.org/pypi/onec_dtools
.. image:: https://img.shields.io/travis/Infactum/onec_dtools/master.svg
    :target: https://travis-ci.org/Infactum/onec_dtools
.. image:: https://img.shields.io/coveralls/Infactum/onec_dtools.svg
    :target: https://coveralls.io/github/Infactum/onec_dtools

**onec_dtools** - библиотека для работы с бинарными файлами 1С:Предприятие 8 (1CD, cf, epf и т.д.) без использования
технологической платформы.

Установка
=========

::

    pip install onec_dtools

Использование
=============

Полное описание всех возможностей библиотеки доступно в документации_.

.. _документации: http://onec-dtools.readthedocs.org/ru/latest/

Простой пример, демонстрирующий чтение всех данных (включая BLOB) из таблицы V8USERS::

    import onec_dtools

    with open('1Cv8.1CD', 'rb') as f:
        db = onec_dtools.DatabaseReader(f)
        if row.is_empty:
                continue
        for row in db.tables['V8USERS']:
            print(row.as_list(True))

Распаковка и запаковки CF файла::

    import onec_dtools

    onec_dtools.extract('D:/sample.cf', 'D:/unpack')
    onec_dtools.build('D:/unpack', 'D:/repacked.cf')


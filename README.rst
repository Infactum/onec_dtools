===========
onec_dtools
===========

.. image:: https://img.shields.io/pypi/v/onec_dtools.svg
    :target: https://pypi.python.org/pypi/onec_dtools
.. image:: https://img.shields.io/pypi/pyversions/onec_dtools.svg
    :target: https://pypi.python.org/pypi/onec_dtools
.. image:: https://img.shields.io/pypi/l/onec_dtools.svg
    :target: https://pypi.python.org/pypi/onec_dtools
.. image:: https://img.shields.io/travis/Infactum/onec_dtools.svg
    :target: https://travis-ci.org/Infactum/onec_dtools
.. image:: https://img.shields.io/coveralls/Infactum/onec_dtools.svg
    :target: https://coveralls.io/github/Infactum/onec_dtools

**onec_dtools** - библиотека для работы с файлами данных 1С:Предприятие 8 (1CD, cf, epf и т.д.) без использования
технологической платформы.

Установка
=========

::

    pip install onec_dtools

Использование
=============

Полное описание применения библиотеки доступно в документации_.

Простой пример, демонстрирующий чтение таблицы V8USERS::

    import onec_dtools

    with open('1Cv8.1CD', 'rb') as f:
        db = onec_dtools.Database(f)

        table_name = 'V8USERS'
        for row in db.read_table(table_name):
            print(row)

.. _документации: http://onec-dtools.readthedocs.org/ru/latest/
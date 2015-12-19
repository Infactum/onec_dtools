===========
onec_dtools
===========

.. image:: https://img.shields.io/pypi/v/onec_dtools.svg
    :target: https://pypi.python.org/pypi/onec_dtools
.. image:: https://img.shields.io/pypi/l/onec_dtools.svg

``onec_dtools`` - библиотека для работы с файлами данных 1С:Предприятие 8 (1CD, cf, и т.д.)

Установка
=========
::

    pip install onec_dtools

Использование
=============
Простой пример, демонстрирующий чтение общей информации о базе данных и вывод содержимого таблицы V8USERS::

    import oneс_dtools

    with open('1Cv8.1CD', 'rb') as f:
        db = oneс_dtools.Database(f)
        print("База данных 1С (вер. {}/{})".format(db.version, db.locale))
        print("Всего таблиц: {}".format(len(db.description)))
    
        table_name = 'V8USERS'
        for row in db.read_table(table_name):
            print(row)

История версий
==============
0.0.1
*****
* Первая публичная версия
* Реализована поддержка чтения формата 1CD

0.0.3
*****
* Поддержка Python 3.4

0.1.0
*****
* Добавлен функционал работы с контейнерами (cf, epf, ert и т.д.)

0.1.1
*****
* Исправление ошибок
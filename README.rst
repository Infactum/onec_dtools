============
1C-dtools
============

``1C-dtools`` - библиотека для работы с файлами данных 1С:Предприятие 8 (1CD, cf, и т.д.)

Установка
===============
::

    pip install 1c-dtools

Использование
===============
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
===============
0.0.1
******************
* Первая публичная версия
* Реализована поддержка чтения формата 1CD
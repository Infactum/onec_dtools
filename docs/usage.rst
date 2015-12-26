.. highlight:: python
   :linenothreshold: 5

Использование
=============

Работа с файлами БД
===================

Рассмотрим работу с файлами базы данных на простом примере, демонстрирующем чтение общей информации о базе и получение
данных о пользователях из таблицы V8USERS. Обратите внимание, что данные полей неограниченной длины не считываются
автоматически при получении данных строки во избежание лишнего расхода памяти. Считывание таких данных необходимо
производить отдельно, используя имеющийся генератор. ::

    import onec_dtools

    with open('1Cv8.1CD', 'rb') as f:
        db = onec_dtools.Database(f)
        print("База данных 1С (вер. {}/{})".format(db.version, db.locale))
        print("Всего таблиц: {}".format(len(db.description)))

        table_name = 'V8USERS'
        for row in db.read_table(table_name):
            data = b''.join([chunk for chunk in row['DATA'].data])
            print('{0[NAME]:25} {1}'.format(row, data))

Работа с контейнерами
=====================

Следующий код реализует возможности распаковки и обратной сборки контейнеров по аналогии с тем, как это делает
v8unpack::

    import argparse
    import sys
    import onec_dtools


    def main():
        parser = argparse.ArgumentParser()
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('-P', '--parse', nargs=2, metavar=('in_filename', 'out_dir_name'))
        group.add_argument('-B', '--build', nargs=2, metavar=('in_dir_name', 'out_filename'))

        if len(sys.argv) == 1:
            parser.print_help()
            return 1

        args = parser.parse_args()

        if args.parse is not None:
            onec_dtools.extract(*args.parse)

        if args.build is not None:
            onec_dtools.build(*args.build)


    if __name__ == '__main__':
        sys.exit(main())


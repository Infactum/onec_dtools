.. highlight:: python
   :linenothreshold: 5

Использование
=============

Работа с файлами БД
-------------------

Для чтения БД используется классс :code:`DatabaseReader`. При инициализации класса считывается основная информация о
БД (версия формата, язык и т.д) и список таблиц. Каждая таблица представляет собой объект класса :code:`Table`.
Обращение к строкам таблицы может выполняться путем итерирования объекта таблицы, либо путем обращения по индексу.
Каждая строка таблицы БД представляет собой объект класса :code:`Row`. Методы работы со строками аналогичны методам
работы с таблицами БД.

Стоит обратить внимание на то, что преобразование значений полей из внутреннего формата 1С происходит при обращении к
полю. В дальнейшем значение кэшируется внутри объект. Таким образом, чтобы не снижать скорость работы, не рекоммендуется
применять методы :code:`Row.as_dict` и :code:`Row.as_list` если не требуются значения всех полей.

Значения полей неограниченной длины представлены объектами класса :code:`Blob`. Значение поля может быть считано в
память целиком путем обращения к свойству :code:`Blob.value`. Если объект слишком большой, чтобы поместиться в памяти
(размер можно получить через :code:`len(Blob)`), то он может быть считан частями по 256 байт путем итерирования.

Следующий пример демонстрирует чтение данных о пользователях (а так же расшифровку хэшей паролей) из таблицы V8USERS
файловой БД. ::

    import binascii
    import re
    import base64
    import argparse
    import onec_dtools


    def extract_hashes(text):
        """
        Получает SHA1 хэши паролей пользователей из расшифрованных данных поля DATA

        :param text: расшифрованное поле DATA
        :return: кортеж хэшей: (SHA1(pwd), SHA1(TO_UPPER(pwd))
        """
        result = re.search('\d+,\d+,"(\S+)","(\S+)",\d+,\d+', text)
        if result is None:
            return
        return tuple([''.join('{:02x}'.format(byte) for byte in base64.decodebytes(x.encode())) for x in result.groups()])


    def decode_data_fld(buffer):
        """
        Декодирование поля DATA таблицы V8USERS

        :param buffer: зашифрованные данные
        :return:
        """
        # Первый байт содержит длину маски шифрования
        # Далее каждая порция байт соответствующей длины поксорена на маску
        mask_length = int(buffer[0])
        j = 1
        decoded = []
        for i in buffer[mask_length + 1:]:
            decoded.append('{:02X}'.format(int(buffer[j] ^ int(i))))
            j += 1
            if j > mask_length:
                j = 1
        decoded_hex_str = ''.join(decoded)
        decoded_bin_str = binascii.unhexlify(decoded_hex_str)
        return decoded_bin_str.decode("utf-8-sig")


    if __name__ == '__main__':
        parser = argparse.ArgumentParser()
        parser.add_argument('path_to_1CD', type=str)
        args = parser.parse_args()
        with open(args.path_to_1CD, 'rb') as f:
            db = onec_dtools.DatabaseReader(f)

            print("+{}+{}+{}+{}+".format(6*'-', 50*'-', 42*'-', 42*'-'))
            print("|{:6}|{:50}|{:42}|{:42}|".format('Админ', 'Имя пользователя', 'SHA1', 'SHA1'))
            print("+{}+{}+{}+{}+".format(6*'-', 50*'-', 42*'-', 42*'-'))

            for row in db.tables['V8USERS']:
                if row.is_empty:
                    continue
                hashes = extract_hashes(decode_data_fld(row['DATA'].value))
                if hashes is None:
                    continue
                print("|{0[ADMROLE]!r:6}|{0[NAME]:50}|{1[0]:42}|{1[1]:42}|".format(row, hashes))

            print("+{}+{}+{}+{}+".format(6*'-', 50*'-', 42*'-', 42*'-'))

Результатом на примере демо базы конфигурации `Управляемое приложение <http://its.1c.ru/db/metod8dev/content/5028/hdoc>`_
будет следующая таблица: ::

    +------+--------------------------------------------------+------------------------------------------+------------------------------------------+
    |Админ |Имя пользователя                                  |SHA1                                      |SHA1                                      |
    +------+--------------------------------------------------+------------------------------------------+------------------------------------------+
    |True  |Администратор                                     |da39a3ee5e6b4b0d3255bfef95601890afd80709  |da39a3ee5e6b4b0d3255bfef95601890afd80709  |
    |False |Менеджер по закупкам                              |da39a3ee5e6b4b0d3255bfef95601890afd80709  |da39a3ee5e6b4b0d3255bfef95601890afd80709  |
    |False |Менеджер по продажам                              |da39a3ee5e6b4b0d3255bfef95601890afd80709  |da39a3ee5e6b4b0d3255bfef95601890afd80709  |
    |False |Продавец                                          |da39a3ee5e6b4b0d3255bfef95601890afd80709  |da39a3ee5e6b4b0d3255bfef95601890afd80709  |
    +------+--------------------------------------------------+------------------------------------------+------------------------------------------+

Работа с контейнерами
---------------------

Работать с контейнерами можно как используя классы :code:`ContainerReader` и :code:`ContainerWriter` для распаковки/упаковки
контейнеров соответственно, так и применяя синтаксический сахар в виде функций :code:`parse` и :code:`build`.

Следующий код реализует возможности распаковки и обратной сборки контейнеров по аналогии с тем, как это делает C++
версия `v8unpack <https://github.com/dmpas/v8unpack>`_::

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


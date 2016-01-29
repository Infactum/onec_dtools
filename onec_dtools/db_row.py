# -*- coding: utf-8 -*-
from struct import unpack
import collections
import datetime as dt

FieldParserInfo = collections.namedtuple('FieldDecoder', 'length, conversion_fn')
RowColumn = collections.namedtuple('RowColumn', 'name, length, offset, conversion_fn')
NText = collections.namedtuple('NText', 'offset, size')
Image = collections.namedtuple('Image', 'offset, size')


def numeric_to_int(numeric, number_of_digits, precision):
    """Преобразуем Numeric формат 1С в число.
    :param numeric: число в формате numeric
    :param number_of_digits: количество цифр в числе
    :param precision: точность
    """
    sign = {0: '-', 1: ''}
    hex_str = ''.join('{:02X}'.format(byte) for byte in numeric)
    if precision:
        result = ''.join([sign.get(int(hex_str[0])), hex_str[1:-precision], '.',
                          hex_str[number_of_digits+1-precision:number_of_digits+1]])
        return float(result)
    else:
        result = ''.join([sign.get(int(hex_str[0])), hex_str[1:number_of_digits+1]])
        return int(result)


def nvc_to_string(nvc):
    """Преобразует NVarChar формат 1С в строку.
    :param nvc: строка в формате NVC
    """
    length, = unpack('H', nvc[:2])
    if not length:
        return ''
    # s тип = 1 байт на символ. У нас по 2 байта, т.к. UTF-16. Все удваиваем.
    fmt = ''.join([str(length * 2), 's'])
    return unpack(fmt, nvc[2:length * 2 + 2])[0].decode('utf-16')


def get_field_parser_info(field_description):
    """
    Возвращает данные для парсера строк файлов записей БД 1С: длину значения колонки (байт) и функцию преобразования
    из массива байт в значение.
    :param field_description: описание полей таблицы БД
    """
    if field_description.type == 'B':
        # Бинарные данные оставляем в чисто виде
        return FieldParserInfo(field_description.length, lambda x: x)
    elif field_description.type == 'L':
        # Булево
        return FieldParserInfo(1, lambda x: unpack('?', x)[0])
    elif field_description.type == 'N':
        # Число
        return FieldParserInfo(field_description.length // 2 + 1,
                               lambda x: numeric_to_int(x, field_description.length, field_description.precision))
    elif field_description.type == 'NC':
        # Строка фиксированной длины
        return FieldParserInfo(field_description.length * 2, lambda x: x.decode('utf-16'))
    elif field_description.type == 'NVC':
        # Строка переменной длины
        return FieldParserInfo(field_description.length * 2 + 2, nvc_to_string)
    elif field_description.type == 'RV':
        # Версия строки
        return FieldParserInfo(16, lambda x: '.'.join(str(i) for i in unpack('4i', x)))
    elif field_description.type == 'NT':
        # Текст неограниченной длины
        return FieldParserInfo(8, lambda x: NText(*unpack('2I', x)))
    elif field_description.type == 'I':
        # Двоичные данные неограниченной длины
        return FieldParserInfo(8, lambda x: Image(*unpack('2I', x)))
    elif field_description.type == 'DT':
        def bytes_to_datetime(bts):
            date_string = ''.join('{:02X}'.format(byte) for byte in bts)
            # У пустой даты год = 0000
            if bts[:2] == b'\x00\x00':
                return None
            return dt.datetime(int(date_string[:4]), int(date_string[4:6]), int(date_string[6:8]),
                               int(date_string[8:10]), int(date_string[10:12]), int(date_string[12:]))
        return FieldParserInfo(7, bytes_to_datetime)


def get_null_field_parser_info(field_description):
    """
    Обертка над get_field_parser_info. Корректирует данные с учетом того, что значение колонки может принимать NULL.
    Для полей, содержащих NULL, функция преобразования будет возвращать None.
    :param field_description: описание полей таблицы БД
    """
    fpi = get_field_parser_info(field_description)
    if field_description.null_exists:
        # Колонки с null_exists меняют значение в зависимости от 1го байта
        return FieldParserInfo(fpi.length + 1, lambda x: None if x[0:1] == b'\x00' else fpi.conversion_fn(x[1:]))
    else:
        return fpi


class Row(object):
    def __init__(self, table_description):
        self.columns = []
        # Длина записи строки (байт)
        self.length = 1
        # Начальное значение смещения для полей с типо != RV
        # Если в таблице есть поле типа RV, то такое поле всегда идет первым
        # Длина поля RV 16 Байт + доп. смещение на 1 из-за байта "чистой записи"
        offset = 17 if table_description.row_version else 1
        for field_name, field_description in table_description.fields.items():
            field_length, conversion_fn = get_null_field_parser_info(field_description)
            self.columns.append(
                RowColumn(field_name, field_length, offset if field_description.type != 'RV' else 1, conversion_fn))
            offset += field_length if field_description.type != 'RV' else 0
            self.length += field_length
        # Длина записи не может быть меньше 5
        self.length = max(self.length, 5)

    def parse(self, buffer):
        """Возвращает словарь колонок и их значений.
        :param buffer: последовательность байт, содержащая данные 1й строки
        """
        return {c.name: c.conversion_fn(buffer[c.offset:c.offset + c.length]) for c in self.columns}

# -*- coding: utf-8 -*-
from struct import unpack, calcsize
import collections
import re
import datetime as dt
import math

ROOT_OBJECT_OFFSET = 2
BLOB_CHUNK_SIZE = 256


class FieldDescription(collections.namedtuple('FieldDescription', 'type, null_exists, length, precision,'
                                                                  ' case_sensitive, data_offset, data_length')):
    """
    Описание поля таблицы

    .. py:attribute:: type

    .. py:attribute:: null_exists

    .. py:attribute:: length

    .. py:attribute:: precision

        Длина дробной части для типа Numeric

    .. py:attribute:: case_sensitive

    .. py:attribute:: data_offset

        Смещение данных поля относительно начала строки (байт)

    .. py:attribute:: data_length

        Длина данных поля (байт)

    """

table_description_pattern_text = '\{"(\S+)".*\n\{"Fields",\n([\s\S]*)\n\},\n\{"Indexes"(?:,|)([\s\S]*)\},' \
                                 '\n\{"Recordlock","(\d)+"\},\n\{"Files",(\S+)\}\n\}'
table_description_pattern = re.compile(table_description_pattern_text)
field_description_pattern = re.compile('\{"(\w+)","(\w+)",(\d+),(\d+),(\d+),"(\w+)"\}(?:,|)')


def database_header(db_file):
    """
    Читает заголовок файла БД

    :param db_file: Объект файла БД
    :type db_file: BufferedReader
    :return: версия и число страниц
    :rtype: tuple
    """
    fmt = '8s4bIi'
    buffer = db_file.read(calcsize(fmt))
    data = unpack(fmt, buffer)

    version = ".".join([str(v) for v in data[1:5]])
    if version not in ['8.2.14.0', '8.3.8.0']:
        error_text = 'Database file has unsupported format version {}'.format(version)
        raise NotImplementedError(error_text)

    total_pages = data[5]

    page_size = 4096
    if version == '8.3.8.0':
        fmt = 'I'
        buffer = db_file.read(calcsize(fmt))
        page_size = unpack(fmt, buffer)[0]

    return version, total_pages, page_size


def root_object(db_file, version, page_size):
    """
    Читает корневой объет БД

    :param db_file: Объект файла БД
    :type db_file: BufferedReader
    :param version: Версия формата БД
    :type version: str
    :param page_size: Размер страницы БД
    :type page_size: int
    :return: язык, описание таблиц БД во внутреннем формате 1С
    :rtype: tuple
    """
    db_object = DBObject(db_file, version, page_size, ROOT_OBJECT_OFFSET)
    if version == '8.3.8.0':
        buffer = Blob(db_file, version, page_size, len(db_object), ROOT_OBJECT_OFFSET, 1, 'I').value
    else:
        buffer = db_object.read()

    fmt = '32si'
    header_size = calcsize(fmt)
    locale, tables_count = unpack(fmt, buffer[:header_size])
    locale = locale.decode('utf-8').rstrip('\x00')

    fmt = ''.join([str(tables_count), 'i'])
    offsets = unpack(fmt, buffer[header_size:header_size + calcsize(fmt)])

    if version == '8.3.8.0':
        tables_descriptions = [(lambda x: x.decode('utf-8'))(
            Blob(db_file, version, page_size, len(db_object), ROOT_OBJECT_OFFSET, offset, 'I').value)
                               for offset in offsets]
    else:
        tables_descriptions = [(lambda x: x.decode('utf-16'))(DBObject(db_file, version, page_size, offset).read())
                               for offset in offsets]

    return locale, tables_descriptions


def calc_field_size(field_type, length):
    """
    Рассчитывает размер данных поля

    :param field_type: Тип поля
    :type field_type: string
    :param length: Длина поля
    :type length: int
    :return: Длина поля в байтах
    :rtype: int
    """
    if field_type == 'B':
        return length
    elif field_type == 'L':
        return 1
    elif field_type == 'N':
        return length // 2 + 1
    elif field_type == 'NC':
        return length * 2
    elif field_type == 'NVC':
        return length * 2 + 2
    elif field_type == 'RV':
        return 16
    elif field_type == 'NT':
        return 8
    elif field_type == 'I':
        return 8
    elif field_type == 'DT':
        return 7
    else:
        raise ValueError('Unknown field type')


def numeric_to_int(numeric, length, precision):
    """
    Преобразуем Numeric формат 1С в число.

    :param numeric: число в формате Numeric
    :type numeric: bytearray
    :param length: длина поля
    :type length: int
    :param precision: точность
    :type precision: int
    :return: Числовое представление
    :rtype: int или float
    """
    sign = {0: '-', 1: ''}
    hex_str = ''.join('{:02X}'.format(byte) for byte in numeric)
    if precision:
        result = ''.join([sign.get(int(hex_str[0])), hex_str[1:-precision], '.',
                          hex_str[length + 1 - precision:length + 1]])
        return float(result)
    else:
        result = ''.join([sign.get(int(hex_str[0])), hex_str[1:length + 1]])
        return int(result)


def nvc_to_string(nvc):
    """
    Преобразует NVarChar формат 1С в строку.

    :param nvc: строка в формате NVC
    :type nvc: bytearray
    :return: Строковое представление
    :rtype: string
    """
    length, = unpack('H', nvc[:2])
    if not length:
        return ''
    # s тип = 1 байт на символ. У нас по 2 байта, т.к. UTF-16. Все удваиваем.
    fmt = ''.join([str(length * 2), 's'])
    return unpack(fmt, nvc[2:length * 2 + 2])[0].decode('utf-16')


def bytes_to_datetime(bts):
    """
    Пробразует данные типа DT в дату/время

    :param bts: значение в формате DT
    :type bts: bytearray
    :return: дата+время
    :rtype: datetime
    """
    date_string = ''.join('{:02X}'.format(byte) for byte in bts)
    # У пустой даты год = 0000
    if bts[:2] == b'\x00\x00':
        return None
    return dt.datetime(int(date_string[:4]), int(date_string[4:6]), int(date_string[6:8]),
                       int(date_string[8:10]), int(date_string[10:12]), int(date_string[12:]))


class DBDescription(object):
    """
    Описание файла БД

    :param db_file: Объект файла БД
    :type db_file: BufferedReader
    """
    def __init__(self, db_file):

        version, total_pages, page_size = database_header(db_file)

        #: Версия формата
        self.version = version
        #: Количество страниц в БД
        self.total_pages = total_pages
        #: Размер страницы
        self.page_size = page_size

        locale, tables_offsets = root_object(db_file, self.version, self.page_size)
        #: Язык БД
        self.locale = locale
        #: Cмещения объектов описания таблиц БД
        self.tables_offsets = tables_offsets


class DBObject(object):
    """
    Объект БД

    :param db_file: Объект файла БД
    :type db_file: BufferedReader
    :param version: Версия формата БД
    :type version: str
    :param page_size: Размер страницы БД
    :type page_size: int
    :param object_offset: смещение объекта БД относительно начала файла БД (в страницах)
    :type object_offset: int
    """
    def __init__(self, db_file, version, page_size, object_offset):
        self._db_file = db_file
        self._version = version
        self._page_size = page_size
        self._db_file.seek(self._page_size * object_offset)

        self._data_pages_offsets = []
        self._length = 0

        if self._version == '8.3.8.0':

            fmt = ''.join(['2sH3IQ', str((self._page_size - calcsize('2sH3IQ')) // calcsize('I')), 'I'])
            buffer = self._db_file.read(self._page_size)
            data = unpack(fmt, buffer)

            # Сигнатура объекта БД версии 8.3.8
            sig = data[0]

            if sig == b'\x1C\xFD':
                # Основные объекты

                # Количество промежуточных слоев таблицы размещения
                fat_level = data[1]
                self._length = data[5]

                if fat_level == 0:
                    data_pages_count = math.ceil(self._length / self._page_size)
                    self._data_pages_offsets += [data[6 + i] for i in range(data_pages_count)]
                elif fat_level == 1:
                    index_pages_offsets = []
                    for i in range(6, len(data)):
                        if data[i] == 0:
                            break
                        index_pages_offsets.append(data[i])

                    for offset in index_pages_offsets:
                        self._db_file.seek(self._page_size * offset)
                        buffer = self._db_file.read(self._page_size)
                        fmt = ''.join([str(self._page_size // calcsize('I')), 'I'])
                        data = unpack(fmt, buffer)
                        for value in data:
                            if value == 0:
                                break
                            self._data_pages_offsets.append(value)
                else:
                    raise NotImplementedError('fat_level {} not supported'.format(fat_level))

            elif sig == b'\x1C\xFF':
                # Объект описания свободных блоков
                error_text = 'Reading of {} type objects is not yet supported'.format(sig)
                raise NotImplementedError(error_text)
            else:
                raise BufferError('Object signature unknown')
        else:
            # Версия формата БД ранее 8.3.8

            assert self._page_size == 4096

            buffer = self._db_file.read(self._page_size)
            data = unpack('8s3iI1018I', buffer)

            assert data[0] == b'1CDBOBV8'

            self._length = data[1]

            index_pages_count = (data[1] - 1) // (1023 * self._page_size) + 1
            index_pages_offsets = [data[5 + i] for i in range(index_pages_count)]

            for offset in index_pages_offsets:
                self._db_file.seek(self._page_size * offset)
                buffer = self._db_file.read(self._page_size)
                data = unpack('i1023I', buffer)
                self._data_pages_offsets += [data[i + 1] for i in range(data[0])]

        # Индекс текущей страницы данных
        self._current_data_page = 0
        # Смещение на текущей страницы данных (байт)
        self._pos_on_page = 0

    def read(self, size=-1):
        """
        Читает не более size байт данных объекта БД

        :param size: Размер считываемых данных. Size < 0 для чтения всего объекта.
        :type size: int
        :return: данные объекта
        :rtype: bytearray
        """
        buffer = []
        # Байт от текущей позиции внутри объета до конца значимых данных
        total_bytes_left = self._length - self._current_data_page * self._page_size - self._pos_on_page

        # Определяем сколько всего байт нужно прочитать
        if size < 0:
            # Читаем весь объект
            bytes_left = total_bytes_left
        else:
            # Читаем SIZE байт, но не более оставшегося числа данных
            bytes_left = min(size, total_bytes_left)

        while bytes_left:
            # Позиционируемся внутри текущей страницы с данными
            self._db_file.seek(self._page_size * self._data_pages_offsets[self._current_data_page] + self._pos_on_page)
            # Определяем сколько байт возможно прочитать: до конца страницы или до оставшегося числа байт
            max_read = min(self._page_size - self._pos_on_page, bytes_left)
            if max_read + self._pos_on_page == self._page_size:
                # Полностью считали страницу данных - переходим на следующую
                self._current_data_page += 1
                self._pos_on_page = 0
            else:
                # Нужное число данных считано ранее конца страницы данных - запоминаем текущую позицию
                self._pos_on_page += max_read
            bytes_left -= max_read
            buffer.append(self._db_file.read(max_read))

        return b''.join(buffer)

    def seek(self, pos):
        """
        Позиционируется на смещении относительно начала данных объекта

        :param pos: Байт от начала данных объекта
        :type pos: int
        """
        if pos > self._length:
            raise IndexError('Position is outside of object')

        # страница данных на которых расположена нужная позиция
        self._current_data_page = pos // self._page_size
        # смещение внутри страницы данных
        self._pos_on_page = pos % self._page_size

    def __len__(self):
        """
        Реализует интерфейс получения размера объета

        :return: Размер объекта в байтах
        :rtype: int
        """
        return self._length


class Table(object):
    """
    Таблица файловой БД

    :param db_file: Объект файла БД
    :type db_file: BufferedReader
    :param description: Описание таблицы во внутреннем формате 1С
    :type description: string
    """
    def __init__(self, db_file, version, page_size, description):
        self._db_file = db_file
        self._version = version
        self._page_size = page_size
        self._db_object = None

        result = table_description_pattern.match(description)
        if result is None:
            raise ValueError("RAW table description doesn't match required format")

        #: Имя таблицы
        self.name = result.group(1)
        self.record_lock = result.group(4) == '1'
        self.data_offset, self.blob_offset, self.index_offset = [int(x) for x in result.group(5).split(",")]
        #: Словарь описаний полей таблицы
        self.fields = collections.OrderedDict()
        # информация об индексах таблицы пока не имеет практического применения, поэтому ее не разбираем
        # текстовое описание индексов в result.group(3)

        offset = 17 if '"RV"' in result.group(2) else 1
        for field_str in result.group(2).splitlines():
            res = field_description_pattern.match(field_str)
            if res is None:
                raise ValueError("RAW field description doesn't match required format")

            name = res.group(1)
            field_type = res.group(2)
            null_exists = res.group(3) == '1'
            length = int(res.group(4))
            precision = int(res.group(5))
            case_sensitive = res.group(6) == 'CS'

            data_length = (1 if null_exists else 0) + calc_field_size(field_type, length)
            if field_type == 'RV':
                data_offset = 1
            else:
                data_offset = offset
                offset += data_length

            self.fields[name] = FieldDescription(field_type, null_exists, length, precision, case_sensitive,
                                                 data_offset, data_length)
        # Длина строки таблицы не может быть меньше 5
        self._row_length = max(offset, 5)

    @property
    def _data_object(self):
        if self._db_object is None:
            self._db_object = DBObject(self._db_file, self._version, self._page_size, self.data_offset)
        return self._db_object

    def __len__(self):
        """
        Позволяет получать число строк в таблице

        :return: Общее количество строк в таблице (включая пустые)
        :rtype: int
        """
        data_object_length = len(self._data_object)
        if data_object_length % self._row_length:
            raise ValueError("Database object length not multiple by row length")
        return data_object_length // self._row_length

    def __iter__(self):
        """
        Реализует интерфейс перебора строк табилцы

        :return: Итератор строк таблицы
        """
        while True:
            row_bytes = self._data_object.read(self._row_length)
            if not row_bytes:
                break
            yield Row(self._db_file, self._version, self._page_size, row_bytes, self)

    def __getitem__(self, key):
        """
        Реализует интерфейс работы с таблицой как со списком

        :param key: индекс строки
        :type key: int
        :return: строка таблицы
        :rtype: Row
        """
        if isinstance(key, int):
            if key >= len(self):
                raise IndexError('Index outside of table length')
            self._db_object.seek(self._row_length * key)
            row_bytes = self._db_object.read(self._row_length)
            return Row(self._db_file, self._version, self._page_size, row_bytes, self)
        else:
            raise TypeError('Index must be int')


class Row(object):
    """
    Строка БД

    :param db_file: Объект файла БД
    :type db_file: BufferedReader
    :param row_bytes: Внутреннее представление строки
    :type row_bytes: bytearray
    :param table: Таблица БД, которой принадлежит строка.
    :type table: Table
    """
    def __init__(self, db_file, version, page_size, row_bytes, table):
        self._row_bytes = row_bytes
        #: Флаг пустой строки. Все поля пустой строки равны None
        self.is_empty = row_bytes[:1] == b'\x01'
        self._version = version
        self._page_size = page_size
        self._db_file = db_file
        self._fields = table.fields
        self._blob_offset = table.blob_offset
        self._fields_values = {}

    def _convert(self, value, field):
        """
        Преобразование внутреннего формата 1С в соответствующий ему формат Python

        :param value: значение во внутреннем формате 1С
        :param field: описание поля таблицы БД
        :return:
        """
        buffer = value
        if field.null_exists:
            if buffer[:1] == b'\x00':
                # Поле не содержит значения (NULL)
                return None
            else:
                # Обрезаем флаг пустого значения
                buffer = buffer[1:]

        if field.type == 'B':
            return buffer
        elif field.type == 'L':
            return unpack('?', buffer)[0]
        elif field.type == 'N':
            return numeric_to_int(buffer, field.length, field.precision)
        elif field.type == 'NC':
            return buffer.decode('utf-16')
        elif field.type == 'NVC':
            return nvc_to_string(buffer)
        elif field.type == 'RV':
            return '.'.join(str(i) for i in unpack('4i', buffer))
        elif field.type in ['NT', 'I']:
            offset, size = unpack('2I', buffer)
            return Blob(self._db_file, self._version, self._page_size, size, self._blob_offset, offset, field.type)
        elif field.type == 'DT':
            return bytes_to_datetime(buffer)

    def __getitem__(self, key):
        """
        Позволяет получать значения полей по имени колонки

        :param key: Имя колонки
        :type key: string
        :return: Значение поля
        """
        if key not in self._fields:
            raise KeyError

        if self.is_empty:
            return None

        if key in self._fields_values:
            return self._fields_values[key]
        else:
            field = self._fields[key]
            field_bytes = self._row_bytes[field.data_offset:field.data_offset + field.data_length]
            result = self._convert(field_bytes, field)
            self._fields_values[key] = result
            return result

    def as_dict(self, read_blobs=False):
        """
        Возвращает представление строки таблицы в виде словаря

        :param read_blobs: Флаг считывания значений BLOB полей
        :type read_blobs: bool
        :return: Строка таблицы
        :rtype: OrderedDict
        """
        res = collections.OrderedDict()
        for name, description in self._fields.items():
            if read_blobs and description.type in ['NT', 'I'] and self[name] is not None:
                res[name] = self[name].value
            else:
                res[name] = self[name]
        return res

    def as_list(self, read_blobs=False):
        """
        Возвращает представление строки таблицы в виде списка

        :param read_blobs: Флаг считывания значений BLOB полей
        :type read_blobs: bool
        :return: Строка таблицы
        :rtype: list
        """
        res = []
        for name, description in self._fields.items():
            if read_blobs and description.type in ['NT', 'I'] and self[name] is not None:
                res.append(self[name].value)
            else:
                res.append(self[name])
        return res


class Blob(object):
    """
    Поле неограниченной длины

    :param db_file: Объект файла БД
    :type db_file: BufferedReader
    :param blob_size: Размер BLOB в байтах
    :type blob_size: int
    :param blob_offset: Смещение объекта BLOB данных таблицы в файле БД (страниц)
    :type blob_offset: int
    :param blob_chunk_offset: Смещение данных внутри BLOB объекта (число блоков по 256 байт)
    :type blob_chunk_offset: int
    :param field_type: тип поля неограниченной длины (I или NT)
    :type field_type: string
    """
    def __init__(self, db_file, version, page_size, blob_size, blob_offset, blob_chunk_offset, field_type):
        self._db_file = db_file
        self._size = blob_size
        self._db_object = DBObject(db_file, version, page_size, blob_offset)
        self._blob_chunk_offset = blob_chunk_offset
        self._field_type = field_type
        self._value = None

    def __len__(self):
        """
        :return: Размер поля в байтах
        :rtype: int
        """
        return self._size

    @property
    def value(self):
        """
        :return: Значение поля
        :rtype: bytearray или string
        """
        if self._value is not None:
            return self._value

        value = b''.join([chunk for chunk in iter(self)])

        if self._field_type == 'NT':
            value = value.decode('utf-16')
        self._value = value

        return value

    def __iter__(self):
        """
        Позволяет считывать данные поля блоками.

        :return: Итератор BLOB кусками по 256 байт
        :rtype: bytearray
        """
        if self._size == 0:
            # Пустой BLOB. И такое бывает.
            yield b''
            raise StopIteration

        self._db_object.seek(BLOB_CHUNK_SIZE * self._blob_chunk_offset)
        while True:
            # Читаем блоки BLOB
            buffer = self._db_object.read(BLOB_CHUNK_SIZE)
            next_block, size, data = unpack('Ih250s', bytes(buffer))

            yield data[:size]

            if next_block == 0:
                break

            self._db_object.seek(BLOB_CHUNK_SIZE * next_block)


class DatabaseReader(object):
    """
    :param db_file: файл базы данных
    :type db_file: BufferedReader
    """
    def __init__(self, db_file):
        self._db_file = db_file

        version, total_pages, page_size = database_header(db_file)

        #: Версия формата
        self.version = version
        #: Количество страниц в БД
        self.total_pages = total_pages
        #: Размер страницы
        self.page_size = page_size

        locale, tables_descriptions = root_object(db_file, self.version, self.page_size)
        #: Язык БД
        self.locale = locale

        self.tables = collections.OrderedDict()
        """
        Словарь таблиц БД.

        Ключ: Имя таблицы

        Значение: Объект класса **Table**
        """
        for description in tables_descriptions:
            table = Table(self._db_file, self.version, self.page_size, description)
            self.tables[table.name] = table

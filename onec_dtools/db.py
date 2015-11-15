# -*- coding: utf-8 -*-
from struct import unpack, calcsize
import collections
from onec_dtools.db_row import Row
import onec_dtools.serialization

PAGE_SIZE = 4096
ROOT_OBJECT_OFFSET = 2
BLOB_CHUNK_SIZE = 256

DBObject = collections.namedtuple('DBObject', 'length, data')
DBHeader = collections.namedtuple('DBHeader', 'version, total_pages')
RootObject = collections.namedtuple('RootObject', 'locale, tables_offsets')
FieldDescription = collections.namedtuple('FieldDescription',
                                          'type, null_exists, length, precision, case_sensitive')
TableDescription = collections.namedtuple('TableDescription',
                                          'name, fields, record_lock, row_version,\
                                           data_offset, blob_offset, index_offset')
NValue = collections.namedtuple('NValue', 'size, data')


def database_header(db_file):
    """Возвращает версию формата и количество страниц БД.
    :param db_file: объект файла БД
    """
    fmt = '8s4bI'
    buffer = db_file.read(calcsize(fmt))
    data = unpack(fmt, buffer)

    db_header = DBHeader(".".join([str(v) for v in data[1:5]]), data[5])

    return db_header


def root_object(db_file):
    """Возвращает язык и кортеж смещений объектов описания таблиц БД.
    :param db_file: объект файла БД
    """
    buffer = read_full_object(db_file, ROOT_OBJECT_OFFSET).data

    fmt = '32si'
    header_size = calcsize(fmt)
    locale, tables_count = unpack(fmt, buffer[:header_size])

    fmt = ''.join([str(tables_count), 'i'])
    tables_offsets = unpack(fmt, buffer[header_size:header_size + calcsize(fmt)])

    return RootObject(locale.decode('utf-8').rstrip('\x00'), tables_offsets)


def read_object_gen(db_file, object_offset, chunk_size=4096):
    """
    Возвращает генератор с данными объета БД.
    Первое значение генератора - размер данных объекта (байт).
    Остальные значения генератора - данные объета, порциями по chunk_size байт.
    :param chunk_size: размер порции (байт)
    :param object_offset: смещение объекта в файле
    :param db_file: объект файла БД
    """

    db_file.seek(PAGE_SIZE * object_offset)
    buffer = db_file.read(PAGE_SIZE)
    data = unpack('8s3iI1018I', buffer)

    # length
    yield data[1]

    index_pages_offsets = [i for i in data[5:] if i > 0]
    data_pages_offsets = []

    for offset in index_pages_offsets:
        db_file.seek(PAGE_SIZE * offset)
        buffer = db_file.read(PAGE_SIZE)
        data = unpack('i1023I', buffer)
        data_pages_offsets += [data[i + 1] for i in range(data[0])]

    buffer = b''
    for offset in data_pages_offsets:
        db_file.seek(PAGE_SIZE * offset)
        buffer += db_file.read(PAGE_SIZE)
        # Возможно приведет к снижению быстродействия при чтении маленькими кусками
        while len(buffer) >= chunk_size:
            yield buffer[:chunk_size]
            buffer = buffer[chunk_size:]


def read_object(db_file, object_offset, chunk_size=4096):
    """Обертка над механизмом кусочного чтения объета БД. Разделяет длину объета и генератор с его данными.
    :param chunk_size: размер порции (байт)
    :param object_offset: смещение объекта в файле
    :param db_file: объект файла БД
    """
    gen = read_object_gen(db_file, object_offset, chunk_size)
    length = next(gen)
    return DBObject(length, gen)


def read_full_object(db_file, object_offset):
    """Возвращает объет БД целиком. Во избежание лишнего расхода памяти применять только для инициализаци.
    :param object_offset: смещение объекта в файле
    :param db_file: объект файла БД
    """
    db_object = read_object(db_file, object_offset)
    return DBObject(db_object.length, b''.join([chunk for chunk in db_object.data]))


def read_blob_from_offset(db_file, blob_offset, blob_chunk_offset):
    """Получает генератор для чтения Blob данных и позиционирует его на указанном смещении.
    :param blob_chunk_offset: смещение порции данных внутри BLOB
    :param blob_offset: смещение BLOB внутри файла БД
    :param db_file: объект файла БД
    """
    blob = read_object(db_file, blob_offset, BLOB_CHUNK_SIZE).data
    for _ in range(blob_chunk_offset):
        next(blob)
    return blob


def read_value_from_blob(db_file, blob_offset, blob_chunk_index, decorator):
    """
    Читает значение blob данных по указанному смещению.
    Возвращает decorator(данные) - необходимо для удобного преобразования значений полей типа Image и NText.
    :param decorator: функция приобразования считанных данных
    :param blob_chunk_index: смещение порции данных внутри BLOB
    :param blob_offset: смещение BLOB внутри файла БД
    :param db_file: объект файла БД
    """
    blob = read_blob_from_offset(db_file, blob_offset, blob_chunk_index)
    while True:
        buffer = next(blob)
        next_block, size, data = unpack('Ih250s', buffer)
        yield decorator(data[:size])
        if next_block == 0:
            break

        if next_block < blob_chunk_index:
            blob = read_blob_from_offset(db_file, blob_offset, next_block)
        elif next_block != blob_chunk_index + 1:
            for _ in range(next_block - blob_chunk_index - 1):
                next(blob)

        blob_chunk_index = next_block


def raw_tables_descriptions(db_file, tables_offsets):
    """Возвращает список со строковыми описаниями (внутренний формат) таблиц БД.
    :param tables_offsets: смещение описания таблиц БД в файле БД
    :param db_file: объект файла БД
    """
    return [read_full_object(db_file, offset).data.decode('utf-16').rstrip('\x00') for offset in tables_offsets]


def parse_table_description(raw_description):
    """Возвращает разобранное описание таблицы БД
    :param raw_description: Строковое опиание таблицы БД
    """
    fields = collections.OrderedDict()
    # показывает, что в таблице есть поле типа RV
    # необходимо для корректного чтения файла данных
    row_version = False
    description = onec_dtools.serialization.deserialize(raw_description)

    for field_description in description[2][1:]:
        name, field_type, null_exists, length, precision, case_sensitive = field_description
        if field_type == 'RV':
            row_version = True
        fields[name] = FieldDescription(field_type, bool(null_exists), length, precision,
                                        True if case_sensitive == 'CS' else False)

    # информация об индексах таблицы пока не имеет практического применения, поэтому ее не разбираем
    return TableDescription(description[0], fields, description[4][1], row_version, *description[5][1:])


class Database(object):
    def __init__(self, db_file):
        self.db_file = db_file

        self.version, self.total_pages = database_header(db_file)
        self.locale, table_offsets = root_object(db_file)

        self.description = {}
        for raw_description in raw_tables_descriptions(db_file, table_offsets):
            table_description = parse_table_description(raw_description)
            self.description[table_description.name] = table_description

    def read_table(self, table_name):
        """Читает таблицу БД построчно.
        :param table_name: Имя таблицы БД
        """
        table_description = self.description[table_name]
        # словарь полей неограниченной длины в текущей таблице
        unlimited_value_columns = {field_name: field_description.type
                                   for field_name, field_description in table_description.fields.items()
                                   if field_description.type == 'I' or field_description.type == 'NT'}

        def decorator(t):
            """Возвращает функцию преобразования данных блоба для полей типа Image и NText.
            :param t: имя типа данных
            """
            if t == 'I':
                return lambda x: x
            if t == 'NT':
                return lambda x: x.decode('utf-16')

        row = Row(table_description)
        table = read_object(self.db_file, table_description.data_offset, row.length)
        if table.length % row.length:
            raise ValueError("Database object length not multiple by row length")
        rows_count = table.length // row.length
        for i in range(rows_count):
            row_bytes = next(table.data)
            # пропускаем свободные записи
            if row_bytes[:1] == b'\x01':
                continue
            parsed_row = row.parse(row_bytes)
            for field_name, field_type in unlimited_value_columns.items():
                gen = read_value_from_blob(self.db_file, table_description.blob_offset,
                                           parsed_row[field_name].offset, decorator(field_type))
                parsed_row[field_name] = NValue(parsed_row[field_name].size, gen)

            yield parsed_row

# -*- coding: utf-8 -*-

from struct import unpack, calcsize
import datetime
import zlib
import tempfile
import os


def read_string(file):
    """
    Считывает строковое значение из undeflated EFD файла.

    :param file: Обрабатываемый файл
    :type file: BufferedReader
    :return: Строковое значение
    :rtype: string
    """
    # Длина строки в символах
    str_len = unpack('I', file.read(4))[0]
    # Длина в байтах. Каждый символ UTF-16 - 2 байта
    size = str_len * 2
    data = unpack(str(size) + 's', file.read(size))[0]
    return data.decode('utf-16')


def read_supply_info(file):
    """
    Считывает информацию о комплекте поставки

    :param file: undeflated EFD файл
    :type file: BufferedReader
    :return: язык, наименование комплекта, наименование поставщика, путь к файлу описания
    :rtype: tuple
    """
    # Назначение этой информации неизвестно
    file.read(4)
    lang = read_string(file)
    supply_name = read_string(file)
    provider_name = read_string(file)
    description_path = read_string(file)
    return lang, supply_name, provider_name, description_path


def read_included_file_info(file):
    """
    Считывает описание вложенного файла

    :param file: undeflated EFD файл
    :type file: BufferedReader
    :return: имя файла, время создания, размер файла (байт)
    :rtype: tuple
    """
    # Назначение этой информации неизвестно
    file.read(4)

    filename = read_string(file)
    filetime = unpack('Q', file.read(8))[0]
    # FILETIME - 64-битовое значение, представляющее число интервалов по 100 наносекунд с 1 января 1601
    timestamp = datetime.datetime(1601, 1, 1) + datetime.timedelta(microseconds=filetime/10)

    # Назначение этой информации неизвестно
    file.read(4)

    file_size = unpack('I', file.read(4))[0]

    return filename, timestamp, file_size


class SupplyReader(object):
    """
    Класс для чтения файлов поставок

    :param file: файл поставки (EFD)
    :type file: BufferedReader
    """

    # Размер блока чтения данных, байт
    CHUNK_SIZE = 10*1024*1024

    def __init__(self, file):
        self.file = file
        self.description = {}
        self.included_files = []

    def unpack(self, output_dir):
        """
        Распаковка файла поставки

        :param output_dir: Каталог распаковки
        :type output_dir: string
        """
        with tempfile.TemporaryFile() as f:
            decompressor = zlib.decompressobj(-15)
            while True:
                chunk = self.file.read(self.CHUNK_SIZE)
                if not chunk:
                    break
                f.write(decompressor.decompress(chunk))
            f.seek(0)

            header, supply_info_count = unpack('II', f.read(8))
            # Во всех исследованных файлах поставок заголовок был равен 1.
            # Возможно это версия формата?
            assert header == 1

            for i in range(supply_info_count):
                lang, supply_name, provider_name, description_path = read_supply_info(f)
                self.description[lang] = supply_name, provider_name, description_path

            included_files_count = unpack('I', f.read(4))[0]
            for i in range(included_files_count):
                self.included_files.append(read_included_file_info(f))

            for included_file in self.included_files:
                src_path, mtime, size = included_file

                # Путь все время указан с \ слэшем (Windows style)
                path = os.path.join(
                    os.path.abspath(output_dir),
                    *src_path.split('\\')
                )

                dir_name = os.path.dirname(path)
                if not os.path.exists(dir_name):
                    os.makedirs(dir_name)

                with open(path, 'wb') as out_file:
                    for i in range(size // self.CHUNK_SIZE):
                        out_file.write(f.read(self.CHUNK_SIZE))
                    out_file.write(f.read(size % self.CHUNK_SIZE))

                timestamp = mtime.timestamp()
                os.utime(path, (timestamp, timestamp))
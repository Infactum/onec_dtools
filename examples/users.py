"""
Пример чтения данных о пользователях файловой ИБ (с распаковкой хэшей) при помощь onec_dtools

Copyright (c) 2016 infactum
"""

# -*- coding: utf-8 -*-
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

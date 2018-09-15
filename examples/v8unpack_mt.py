# -*- coding: utf-8 -*-

"""
Пример многопоточной распаковки контейнеров (cf/epf/ert) при помощи onec_dtools
Для "больших" конфигураций уровня ERP требуется использовать Python x64

Copyright (c) 2018 infactum
"""

import os
import io
import zlib
import argparse
import sys
from multiprocessing import Pool, cpu_count
from onec_dtools.container_reader import read_entries


def deflate(name, data):
    return name, zlib.decompressobj(-15).decompress(data)


def write(filename, data):
    with open(filename, 'wb') as f:
        f.write(data)


def extract(filename, path):
    if os.path.exists(path) and os.path.isdir(path):
        os.rmdir(path)
    os.makedirs(path, exist_ok=True)

    deflate_pool = Pool(processes=cpu_count())
    writer_pool = Pool(processes=1)

    with open(filename, 'rb') as f:

        a = [(k, b''.join([c for c in v.data])) for k, v in read_entries(f).items()]
        unpacked = deflate_pool.starmap(deflate, a)

        for name, data in unpacked:
            if data[:4] == b'\xFF\xFF\xFF\x7F':
                os.makedirs(os.path.join(path, name))
                for subname, subobj in read_entries(io.BytesIO(data)).items():
                    subpath = os.path.join(path, name, subname)
                    subdata = b''.join([c for c in subobj.data])
                    writer_pool.apply_async(write, (subpath, subdata))
            else:
                writer_pool.apply_async(write, (os.path.join(path, name), data))

    deflate_pool.close()
    writer_pool.close()
    deflate_pool.join()
    writer_pool.join()


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-U', '--unpack', nargs=2, metavar=('in_filename', 'out_dir_name'))

    if len(sys.argv) == 1:
        parser.print_help()
        return 1

    args = parser.parse_args()

    if args.unpack is not None:
        extract(*args.unpack)


if __name__ == '__main__':
    sys.exit(main())

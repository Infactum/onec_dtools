"""
Пример распаковки/упаковки контейнеров (cf/epf/ert) при помощь onec_dtools
Функционал аналогичен C++ версии v8unpack

Copyright (c) 2016 infactum
"""

# -*- coding: utf-8 -*-
import argparse
import sys
from onec_dtools import extract, build


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-U', '--unpack', nargs=2, metavar=('in_filename', 'out_dir_name'))
    group.add_argument('-B', '--build', nargs=2, metavar=('in_dir_name', 'out_filename'))

    if len(sys.argv) == 1:
        parser.print_help()
        return 1

    args = parser.parse_args()

    if args.unpack is not None:
        extract(*args.unpack)

    if args.build is not None:
        build(*args.build)


if __name__ == '__main__':
    sys.exit(main())
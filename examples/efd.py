# -*- coding: utf-8 -*-

"""
Пример распаковки efd файла поставки

Copyright (c) 2017 infactum
"""

import argparse
import sys
import onec_dtools


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('path_to_EFD', type=str)
    parser.add_argument('output_path', type=str)
    args = parser.parse_args()
    with open(args.path_to_EFD, 'rb') as f:
        supply_reader = onec_dtools.SupplyReader(f)
        supply_reader.unpack(args.output_path)

if __name__ == '__main__':
    sys.exit(main())

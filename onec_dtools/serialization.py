# -*- coding: utf-8 -*-
import ast


def deserialize(serialized_string):
    """Десериализует внутреннее представление данных 1С в список.
    :param serialized_string: строка сериализованных данных
    """
    temp_str = serialized_string.replace('{', '[').replace('}', ']')
    return ast.literal_eval(temp_str)

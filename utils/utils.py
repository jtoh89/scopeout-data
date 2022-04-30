import datetime
import math
import pandas as pd
from lookups import MONTH_STRING_TO_INT

def zero_to_null(value):
    if value == 0:
        return None
    else:
        return value

def truncate_decimals(f, n):
    '''Truncates/pads a float f to n decimal places without rounding'''
    s = '{}'.format(f)
    if 'e' in s or 'E' in s:
        return '{0:.{1}f}'.format(f, n)
    i, p, d = s.partition('.')
    return float('.'.join([i, (d+'0'*n)[:n]]))

def list_float_to_percent(float_list):
    return_list = []
    for value in float_list:
        if value == None:
            return_list.append(None)
        else:
            return_list.append(round(value*100, 1))
    return return_list


def number_to_string(data_type, value):
    if value != value or value is None:
        return None


    if data_type == "dollar":
        return "${:,.0f}".format(value)
    elif data_type == "percent":
        return str(value) + "%"
    # elif data_type == "dollar":
    #     return value

def string_to_int(value):
    if value == '':
        return None

    return math.trunc(float(value))


def string_to_float(value, decimal_places):
    if value == '':
        return None

    return round(float(value), decimal_places)

def nat_to_none(value):
    if value is pd.NaT:
        return None
    return value

def isNaN(num):
    return num != num

def drop_na_values_from_dict(dict):
    return {k: v for k, v in dict.items() if v == v}

def set_na_to_false_from_dict(dict):
    for k, v in dict.items():
        if v != v:
            dict[k] = False


def list_length_okay(list, limit):
    if len(list) > limit:
        return False
    return True

def calculate_percent_change(starting_data, ending_data, move_decimal=True, decimal_places=1):
    if starting_data == 0 or ending_data == 0:
        return 0

    if move_decimal:
        percent_change = round((ending_data - starting_data) / starting_data * 100, decimal_places)
    else:
        percent_change = round((ending_data - starting_data) / starting_data, decimal_places)


    return percent_change


def month_string_to_datetime(month_string):
    month_string = month_string.split(' ')
    month_num = MONTH_STRING_TO_INT[month_string[0]]
    year_num = int(month_string[1])

    date = datetime.datetime(year_num, month_num, 1)

    return date
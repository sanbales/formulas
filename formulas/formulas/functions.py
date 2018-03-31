#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Copyright 2016-2017 European Commission (JRC);
# Licensed under the EUPL (the 'Licence');
# You may not use this work except in compliance with the Licence.
# You may obtain a copy of the Licence at: http://ec.europa.eu/idabc/eupl

"""
Python equivalents of various excel functions.
"""
import functools
import collections
import math
import numpy as np
import schedula as sh
from ..errors import FunctionError, FoundError
from ..tokens.operand import XlError, Error


ufuncs = {item: getattr(np, item) for item in dir(np)
          if isinstance(getattr(np, item), np.ufunc)}


def _replace_empty(x, empty=0):
    if isinstance(x, np.ndarray):
        y = x.ravel().tolist()
        if sh.EMPTY in y:
            y = [empty if v is sh.EMPTY else v for v in y]
            return np.asarray(y, object).reshape(*x.shape)
    return x


def xpower(number, power):
    if number == 0:
        if power == 0:
            return Error.errors['#NUM!']
        if power < 0:
            return Error.errors['#DIV/0!']
    return np.power(number, power)

ufuncs['power'] = xpower


def xarctan2(x, y):
    return x == y == 0 and Error.errors['#DIV/0!'] or np.arctan2(x, y)

ufuncs['arctan2'] = xarctan2


def xmod(x, y):
    return y == 0 and Error.errors['#DIV/0!'] or np.mod(x, y)

ufuncs['mod'] = xmod


def is_number(number):
    if not isinstance(number, Error):
        try:
            float(number)
        except (ValueError, TypeError):
            return False
    return True


def flatten(l, check=is_number):
    if isinstance(l, collections.Iterable) and not isinstance(l, str):
        for el in l:
            yield from flatten(el, check)
    elif not check or check(l):
        yield l


def xsumproduct(*args):
    # Check all arrays are the same length
    # Excel returns #VAlUE! error if they don't match
    assert len(set(arg.size for arg in args)) == 1
    inputs = []
    for a in args:
        a = a.ravel()
        x = np.zeros_like(a, float)
        b = np.vectorize(is_number)(a)
        x[b] = a[b]
        inputs.append(x)

    return np.sum(np.prod(inputs, axis=0))


def xsum(*args):
    return sum(list(flatten(args)))


def xmax(*args):
    return max([arg for arg in flatten(args) if is_number(arg)])


def xmin(*args):
    return min([arg for arg in flatten(args) if is_number(arg)])


def average(*args):
    l = list(flatten(args))
    return sum(l) / len(l)


def lookup(lookup_value, lookup_vector, result_vector=None):
    """
    The vector form of LOOKUP looks in a one-row or one-column range (known as a vector) for a
    value and returns a value from the same position in a second one-row or one-column range.

    :param lookup_value: A value that LOOKUP searches for in the first vector.
    :param lookup_vector: A range that contains only one row or one column.
    :param result_vector: A range that contains only one row or column.

    :type lookup_value: a number, text, a logical value, or a name or reference that refers to a value.
    :type lookup_vector: an array containing text, numbers, or logical values.
    :type result_vector: must be the same size as lookup_vector.

    :return:
    """
    print('\n\n\n')
    print('LOOKUP({}, {}, {})'.format(lookup_value, lookup_vector, result_vector))

    result_vector = lookup_vector if result_vector is None else result_vector

    index = np.where(np.ravel(lookup_vector) == np.ravel(lookup_value))[0]
    print('INDEX = {}'.format(index))
    print('\n\n\n')
    if len(index) > 0:
        return np.ravel(result_vector)[index[0]]
    else:
        return Error.errors['#N/A']


def match(lookup_value, lookup_array, match_type=1):
    """
    Searches for a specified item in a range of cells,
    and then returns the relative position of that item in the range.

    :param lookup_value: The value that you want to match in lookup_array.
    :param lookup_array: The range of cells being searched.
    :param match_type: Specifies how Excel matches lookup_value with values
        in lookup_array. (default: 1)

    :return: The index of the first instance of lookup_value in lookup_array.

    """
    def type_convert(val):
        if isinstance(val, str):
            val = val.upper()
        elif is_number(val):
            val = float(val)
        return val

    lookup_value = type_convert(lookup_value)

    if match_type == 1:
        # Verify ascending sort
        pos_max = -1
        for i in range((len(lookup_array))):
            current = type_convert(lookup_array[i])
            if i is not len(lookup_array)-1 and current > type_convert(lookup_array[i+1]):
                raise ValueError('for match_type 0, lookup_array must be sorted ascending')
            if current <= lookup_value:
                pos_max = i
        if pos_max == -1:
            raise ValueError('No result in lookup_array for match_type 0')
        # Excel starts at 1
        return pos_max + 1

    elif match_type == 0:
        # No string wildcard
        return [type_convert(x) for x in lookup_array].index(lookup_value) + 1

    elif match_type == -1:
        # Verify descending sort
        pos_min = -1
        for i in range((len(lookup_array))):
            current = type_convert(lookup_array[i])
            if i is not len(lookup_array)-1 and current < type_convert(lookup_array[i+1]):
                raise ValueError('For match_type 0, lookup_array must be sorted descending')
            if current >= lookup_value:
                pos_min = i
        if pos_min == -1:
            raise Exception('no result in lookup_array for match_type 0')
        # Excel starts at 1
        return pos_min + 1


def hlookup(lookup_value, table_array, row_index_num, range_lookup=True):
    return vlookup(lookup_value, np.transpose(table_array), row_index_num, range_lookup)


def vlookup(lookup_value, table_array, col_index_num, range_lookup=True):
    """
    Use VLOOKUP, one of the lookup and reference functions, when you need to find
    things in a table or a range by row. For example, look up a price of an
    automotive part by the part number.

    In its simplest form, the VLOOKUP function says:

    =VLOOKUP(Value you want to look up, range where you want to lookup the value,
        the column number in the range containing the return value,
        Exact Match or Approximate Match – indicated as 0/FALSE or 1/TRUE).

    :param lookup_value: A value that VLOOKUP searches for in the first column.
    :param table_array: A range that contains the value being looked up, and the target value.
    :param col_index_num: The number of columns to look to the right of, 1 is looked up value.
    :param range_lookup: Search for an approximate match? (default: True)
    :return:

    """
    if isinstance(lookup_value, (list, tuple, np.ndarray)):
        lookup_value = lookup_value[0]

    if range_lookup:
        idx = -1
        if is_number(lookup_value):
            for idx in range(len(table_array) - 1):
                if all((table_array[idx][0] <= lookup_value,
                        table_array[idx + 1][0] > lookup_value)):
                    return table_array[idx][col_index_num - 1]
        else:
            for idx in range(len(table_array) - 1):
                if all((table_array[idx][0],
                        table_array[idx+1][0])):
                    return table_array[idx][col_index_num - 1]
        return table_array[idx + 1][col_index_num - 1]

    else:
        values = [row[0] for row in table_array]
        if lookup_value in values:
            return table_array[values.index(lookup_value)][col_index_num - 1]
    return None


# noinspection PyUnusedLocal
def not_implemented(*args, **kwargs):
    raise FunctionError()


class Array(np.ndarray):
    pass


def iserr(val):
    try:
        b = np.asarray([isinstance(v, XlError) and v is not Error.errors['#N/A']
                        for v in val.ravel().tolist()], bool)
        b.resize(val.shape)
        return b
    except AttributeError:  # val is not an array.
        return iserr(np.asarray([[val]], object))[0][0]


def iserror(val):
    try:
        b = np.asarray([isinstance(v, XlError)
                        for v in val.ravel().tolist()], bool)
        b.resize(val.shape)
        return b
    except AttributeError:  # val is not an array.
        return iserror(np.asarray([[val]], object))[0][0]


def iferror(val, val_if_error):
    return np.where(iserror(val), val_if_error, val)


def raise_errors(*args):
    # noinspection PyTypeChecker
    for v in flatten(args, None):
        if isinstance(v, XlError):
            raise FoundError(err=v)


def call_ufunc(ufunc, *args):
    """
    Calls a numpy universal function (ufunc) with the specified arguments.

    :param ufunc: the numpy universal function to wrap.
    :param args: arguments to be passed to the ufunc.
    :type ufunc: :class:`numpy.ufunc`
    :type args: tuple
    :return: result from ufunc for given arguments

    """
    def safe_eval(*vals):
        try:
            r = ufunc(*map(float, vals))
            if not isinstance(r, XlError) and (np.isnan(r) or np.isinf(r)):
                r = Error.errors['#NUM!']
        except (ValueError, TypeError):
            r = Error.errors['#VALUE!']
        return r

    res = np.vectorize(safe_eval, otypes=[object])(*map(_replace_empty, args))
    return res.view(Array)


def wrap_func(func, args_indices=None):
    if func in ufuncs:
        func = functools.partial(call_ufunc, ufuncs[func])

    def wrapper(*args, **kwargs):
        # noinspection PyBroadException
        try:
            args = args_indices and [args[i] for i in args_indices] or args
            raise_errors(*args)
            return func(*args, **kwargs)
        except FoundError as ex:
            return np.asarray([[ex.err]], object)
        except:
            return np.asarray([[Error.errors['#VALUE!']]], object)
    return functools.update_wrapper(wrapper, func)


FUNCTIONS = collections.defaultdict(lambda: not_implemented)
FUNCTIONS.update({
    'ABS': wrap_func('abs'),
    'ACOS': wrap_func('arccos'),
    'ACOSH': wrap_func('arccosh'),
    'ARRAY': lambda *args: np.asarray(args, object).view(Array),
    'ARRAYROW': lambda *args: np.asarray(args, object).view(Array),
    'ASIN': wrap_func('arcsin'),
    'ASINH': wrap_func('arcsinh'),
    'ATAN': wrap_func('arctan'),
    'ATAN2': wrap_func('arctan2', (1, 0)),
    'ATANH': wrap_func('arctanh'),
    'AVERAGE': wrap_func(average),
    'COS': wrap_func('cos'),
    'COSH': wrap_func('cosh'),
    'DEGREES': wrap_func('degrees'),
    'EXP': wrap_func('exp'),
    'HLOOKUP': wrap_func(hlookup),
    'IF': wrap_func(lambda c, x=True, y=False: np.where(c, x, y)),
    'IFERROR': iferror,
    'INT': wrap_func(int),
    'ISERR': iserr,
    'ISERROR': iserror,
    'LOG': wrap_func('log10'),
    'LN': wrap_func('log'),
    'LOOKUP': wrap_func(lookup),
    'MATCH': wrap_func(match),
    'MAX': wrap_func(xmax),
    'MIN': wrap_func(xmin),
    'MOD': wrap_func('mod'),
    'PI': lambda: math.pi,
    'POWER': wrap_func('power'),
    'RADIANS': wrap_func('radians'),
    'SIN': wrap_func('sin'),
    'SINH': wrap_func('sinh'),
    'SUMPRODUCT': wrap_func(xsumproduct),
    'SQRT': wrap_func('sqrt'),
    'SUM': wrap_func(xsum),
    'TAN': wrap_func('tan'),
    'TANH': wrap_func('tanh'),
    'VLOOKUP': wrap_func(vlookup),
})

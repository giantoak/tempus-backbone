# coding: utf-8
from __future__ import print_function
import sqlalchemy
from sqlalchemy import func
from initdb import tables, table_cols, session
import datetime
import logging
import math
import numpy as np


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig()


def groupby(table, xs, ys, agg, **kwargs):
    '''
    Returns result of an aggregation function applied to groups.

    Parameters
    ----------
    table : str
        Tempus table name.
    ss : str or iterable of str
        Variable(s) to group by.
    ys : str or iterable of str
        Response variable(s) to send to the aggregate function.
    agg : str
        Aggregation function name, from list of valid MySQL aggregation
        functions.
    tstart : datetime.datetime, optional
        Only include entries after `tstart` (the default is to include
        everything)
    tend : datetime.datetime, optional
        Only include entries before `tend` (the default is to include
        everything)

    Returns
    -------
    dict
        Dictionary mapping group identifiers (column names) to a dictionary of
        aggregated response variables.

    See Also
    --------
    groupdo: Performs aggregation function across entire table.

    Notes
    -----
    Valid agg function strings:
        AVG -- Return the average value of the argument
        BIT_AND -- Return bitwise and
        BIT_OR -- Return bitwise or
        BIT_XOR -- Return bitwise xor
        COUNT(DISTINCT)Return the count of a number of different values
        COUNT -- Return a count of the number of rows returned
        GROUP_CONCAT -- Return a concatenated string
        MAX -- Return the maximum value
        MIN -- Return the minimum value
        STD -- Return the population standard deviation
        STDDEV_POP -- Return the population standard deviation
        STDDEV_SAMP -- Return the sample standard deviation
        STDDEV -- Return the population standard deviation
        SUM -- Return the sum
        VAR_POP -- Return the population standard variance
        VAR_SAMP -- Return the sample variance
        VARIANCE -- Return the population standard variance

    '''
    logger.debug('Executing groupby: kwargs={}'.format(kwargs))
    if isinstance(xs, basestring):
        xs = [xs]
    if isinstance(ys, basestring):
        ys = [ys]

    t = tables[table]
    columns = [getattr(t, x) for x in xs]

    yvars = [getattr(t, y) for y in ys]
    f = getattr(func, agg)

    tstart = kwargs.get('tstart', None)
    tend = kwargs.get('tend', None)

    conds = []
    if tstart and tend:
        ts = getattr(t, table_cols[table]['timestamp'])

        conds = [(ts > tstart), (ts < tend)]

    funcs = [f(y) for y in yvars]
    q = session.query(*(funcs + columns))
    for col in columns:
        q = q.group_by(col)

    for cond in conds:
        q = q.filter(cond)

    counts = q.all()
    countdict = {}
    for row in counts:
        yval = row[:len(ys)]
        xs = tuple(row[len(ys):])

        countdict[xs] = dict(zip(ys, yval))

    return countdict


def groupdo(table, yn, *aggs, **kwargs):
    '''
    Returns result of an aggregation function applied to the entire population.

    Parameters
    ----------
    table : str
        Tempus table name.
    yn : str
        Response variable to send to the aggregate function.
    aggs : list of strings
        Aggregation function names, from list of valid MySQL aggregation
        functions.
    tstart : datetime.datetime, optional
        Only include entries after `tstart` (the default is to include
        everything)
    tend : datetime.datetime, optional
        Only include entries before `tend` (the default is to include
        everything)

    Returns
    -------
    list
        List of responses to the SQL query. Since most aggregation functions
        only return one value, this is often a list of a single tuple, the
        first entry of which is the result.

    See Also
    --------
    groupby: Performs aggregation function within groups.

    Notes
    -----
    Valid agg function strings:
        AVG -- Return the average value of the argument
        BIT_AND -- Return bitwise and
        BIT_OR -- Return bitwise or
        BIT_XOR -- Return bitwise xor
        COUNT(DISTINCT)Return the count of a number of different values
        COUNT -- Return a count of the number of rows returned
        GROUP_CONCAT -- Return a concatenated string
        MAX -- Return the maximum value
        MIN -- Return the minimum value
        STD -- Return the population standard deviation
        STDDEV_POP -- Return the population standard deviation
        STDDEV_SAMP -- Return the sample standard deviation
        STDDEV -- Return the population standard deviation
        SUM -- Return the sum
        VAR_POP -- Return the population standard variance
        VAR_SAMP -- Return the sample variance
        VARIANCE -- Return the population standard variance

    '''

    logger.debug('Executing groupdo: kwargs={}'.format(kwargs))
    t = tables[table]

    fs = [getattr(func, agg) for agg in aggs]
    y = getattr(t, yn)

    tstart = kwargs.get('tstart', None)
    tend = kwargs.get('tend', None)

    conds = []
    if tstart and tend:
        ts = getattr(t, table_cols[table]['timestamp'])

        conds = [(ts > tstart), (ts < tend)]

    q = session.query(*[f(y) for f in fs])

    for cond in conds:
        q = q.filter(cond)

    aggs = q.all()

    return aggs


def outliers(table, xs, y, **kwargs):
    '''
    Finds outliers of a response variable to a baseline.

    Outliers of responses grouped by categorical variables are given with
    respect to the population.

    Parameters
    ----------
    table : str
        Tempus table name.
    xs : str or iterable of str
        Categorical variables that are used to group the data.
    y : str, optional
        Response variable to send to the aggregate function (the default is
        `price_col` in the Tempus configuration file).
    tstart : datetime.datetime, optional
        Only include entries after `tstart` (the default is to include
        everything)
    tend : datetime.datetime, optional
        Only include entries before `tend` (the default is to include
        everything)

    Returns
    -------
    outliers: list
    List of outliers formatted as tuples of (group identifier (`xs`), value)

    See Also
    --------
    outliers_in: Outliers of group responses to an aggregation function.

    Notes
    -----
    The outlier is defined as a value 2 standard deviations away from the mean.
    Thus this function is only suitable for continuous variables.

    '''

    # Get average and standard deviation from data
    avg, std = groupdo(table, y, 'AVG', 'STD', **kwargs)[0]

    avgs = groupby(table, xs, y, 'AVG', **kwargs)
    outliers = []

    logger.debug('Groupdo results: avg {}, std {}'.format(avg, std))
    logger.debug('Length of outlier averages: {}'.format(len(avgs)))
    for group in avgs:
        if avgs[group][y] and abs(avgs[group][y] - avg) > 2*std:
            outliers.append((group, avgs[group]))

    return outliers


def outlier_in(table, xs, agg, y=None, **kwargs):
    '''
    Finds outliers in a table aggregation.

    Outliers are returned from the output of an aggregate function.

    Parameters
    ----------
    table : str
        Tempus table name.
    xs : str or iterable of str
        Categorical variables that are used to group the data.
    y : str, optional
        Response variable to send to the aggregate function (the default is
        `price_col` in the Tempus configuration file).
    agg : str
        Aggregation function name, from list of valid MySQL aggregation
        functions.
    tstart : datetime.datetime, optional
        Only include entries after `tstart` (the default is to include
        everything)
    tend : datetime.datetime, optional
        Only include entries before `tend` (the default is to include
        everything)

    Returns
    -------
    outliers: list
    List of outliers formatted as tuples of (group identifier (`xs`), value)

    See Also
    --------
    outlier: Outliers of group responses to a baseline.

    Notes
    -----
    The outlier is defined as a value 2 standard deviations away from the mean.
    Thus this function is only suitable for continuous variables.

    Valid agg function strings:
        AVG -- Return the average value of the argument
        BIT_AND -- Return bitwise and
        BIT_OR -- Return bitwise or
        BIT_XOR -- Return bitwise xor
        COUNT(DISTINCT)Return the count of a number of different values
        COUNT -- Return a count of the number of rows returned
        GROUP_CONCAT -- Return a concatenated string
        MAX -- Return the maximum value
        MIN -- Return the minimum value
        STD -- Return the population standard deviation
        STDDEV_POP -- Return the population standard deviation
        STDDEV_SAMP -- Return the sample standard deviation
        STDDEV -- Return the population standard deviation
        SUM -- Return the sum
        VAR_POP -- Return the population standard variance
        VAR_SAMP -- Return the sample variance
        VARIANCE -- Return the population standard variance


    '''
    y = y or table_cols[table]['price']
    results = groupby(table, xs, y, agg, **kwargs)
    outliers = []

    resps = [results[j][y] for j in results]
    avg, std = np.mean(resps), np.std(resps)

    logger.debug('outlier_in: {}, {}, avg: {}, std: {}'.format(y, agg, avg,
                                                               std))
    outliers = []
    for r in results:
        if results[r][y] > avg + 2*std:
            outliers.append((r, results[r][y]))

    return outliers

if __name__ == '__main__':
    o = outlier_in('escort_ads', 'msaname', 'COUNT',
                   tstart=datetime.datetime(2014, 1, 1),
                   tend=datetime.datetime(2014, 9, 1))

    print(o)

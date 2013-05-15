'''
Created on Sep 27, 2010

@author: syarc
'''
import cheuvreux.utils.statistics as statistics
import math
import operator

class TimeSeries(object):

    def __init__(self):
        self._data = dict()

    def get_sorted_values(self):
        return sorted(self._data.iteritems(),
                      key=operator.itemgetter(0))

    def dates(self):
        return set(self._data.keys())

    def values(self):
        return self._data.values()

    def __iter__(self):
        return self._data.__iter__()

    def __len__(self):
        return len(self._data)

    def __getitem__(self, date):
        return self._data[date]

    def __setitem__(self, date, value):
        self._data[date] = value

    def __contains__(self, date):
        return date in self._data

def intersect(t1, t2):
    ''' Compute the intersection between two timeseries '''
    new_t1 = TimeSeries()
    new_t2 = TimeSeries()

    for date in [date for date in t1 if date in t2]:
        #if date in t2:
        new_t1._data[date] = t1._data[date]
        new_t2._data[date] = t2._data[date]
    return new_t1, new_t2

def log_return(x):
    ts = TimeSeries()

    previous_value = None

    for date, value in x.get_sorted_values():
        if not previous_value:
            previous_value = value
        else:
            ts._data[date] = math.log(value/previous_value)
            previous_value = value

    return ts


# ------------ Statistics function ----------------- #
def covariance(ts1, ts2):
    ''' Compute the covariance of ts1 and ts2 '''
    ts1, ts2 = intersect(ts1, ts2)
    log_ts1, log_ts2 = log_return(ts1), log_return(ts2)

    return statistics.covariance(log_ts1.values(), log_ts2.values())

def correlation(ts1, ts2):
    ''' Compute the correlation of ts1 and ts2 '''
    ts1, ts2 = intersect(ts1, ts2)
    log_ts1, log_ts2 = log_return(ts1), log_return(ts2)

    cov = statistics.covariance(log_ts1.values(), log_ts2.values())
    sd1 = math.sqrt(statistics.variance(log_ts1.values()))
    sd2 = math.sqrt(statistics.variance(log_ts2.values()))

    if sd1 == 0 or sd2 == 0:
        return 0

    return cov / (sd1 * sd2)



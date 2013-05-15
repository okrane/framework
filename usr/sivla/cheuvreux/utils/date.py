'''
Created on 1 avr. 2010

@author: syarc
'''

import datetime
from datetime import timedelta
from datetime import date

def workday_range(startDate, endDate):
    '''
        Helper functions that call 'range' with a filter on work days.
    '''
    return daterange(startDate, endDate, [0, 1, 2, 3, 4])

def daterange(start_date, end_date, day_only=[0, 1, 2, 3, 4, 5, 6]):
    '''
        Returns a list of date between start_date and end_date.

        The additional parameters, day_only, filter date on weekday.
        "range(start_date, end_date, [5,6])" will return only Saturday
        and Sunday between start and end date.
    '''
    for n in range((end_date - start_date).days + 1):
        d = start_date + timedelta(n)
        if d.weekday() in day_only:
            yield d

def parse_date(string):
    ''' Transform a string (YYYYMMDD) to a datetime.date object '''
    return datetime.date(int(string[0:4]),
                         int(string[4:6]),
                         int(string[6:8]))

def previous_weekday(d=None):
    '''
        Returns the last weekday.

        @param d date, if None, set to "today"
    '''
    if d is None:
        d = date.today()

    d = d - timedelta(1)

    while d.weekday() in [5, 6]:
        d = d - timedelta(1)
    return d

def go_ahead(date, days):
    '''
        Add "days" working days to "date" and returns the date.

        @param date: reference date
        @params days: number of weekdays
    '''
    delta = datetime.timedelta(days = days + 2 * (days // 5))
    if date.weekday() == 5:
        delta += datetime.timedelta(days=1)
    elif date.weekday() == 6:
        delta += datetime.timedelta(days=2)
    else:
        leftover = date.weekday() + days % 5
        if leftover >= 5:
            delta += datetime.timedelta(days=2)

    return date + delta

def nb_working_days(startdate, enddate):
    '''
        @return Returns the number of working day between startdate and enddate
    '''
    if type(startdate) is str:
        startdate = datetime.datetime.strptime(startdate, '%Y%m%d').date()

    if type(enddate) is str:
        enddate = datetime.datetime.strptime(enddate, '%Y%m%d').date()

    return len([d for d in workday_range(startdate, enddate)])

def go_back(date, days):
    '''
        Removes "days" working days to "date" and returns the date.

        @param date: reference date
        @params days: number of weekdays
    '''

    delta = datetime.timedelta(days=days + 2 * (days // 5))
    if date.weekday() == 5:
        delta += datetime.timedelta(days=1)
    elif date.weekday() == 6:
        delta += datetime.timedelta(days=2)
    else:
        leftover = date.weekday() - days % 5
        if leftover < 0:
            delta += datetime.timedelta(days=2)

    return date - delta

def next_weekday(d=None):
    '''
        Returns the next working day.

        @param d date, if None, set to "today"
    '''
    if d is None:
        d = date.today()

    d = d + timedelta(1)

    while d.weekday() in [5, 6]:
        d = d + timedelta(1)
    return d

def toMicroseconds(timedelta):
    '''
        Convert a timedelta object to microseconds.
    '''
    return timedelta.days * 86400000000 + timedelta.seconds * 1000000 + timedelta.microseconds


if __name__ == '__main__':
    print nb_working_days(datetime.date(2010,9,17), datetime.date.today())
    #end = previous_weekday(datetime.date.today())
    #start = go_back(end,1)
    #print start, end

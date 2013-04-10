'''
Created on 18 aout 2010

@author: benca
'''


from usr.dev.sivla.funcs.DBTools.tickdb import TickDB
from usr.dev.benca.funcs.DBTools.securities_tools import SecuritiesTools
from simep.funcs.data.pyData import pyData
from simep.funcs.stdio.utils import pyLog
from datetime import datetime, timedelta



class BasicIndicators:
    
    @staticmethod
    def get_volume_slices(security, minutes_per_slice, from_date, to_date):
        # convert
        one_day                = timedelta(days=1)
        timestep               = timedelta(minutes=minutes_per_slice)
        this_is_the_first_date = True
        datetimes              = []
        values                 = {}
        if isinstance(from_date, str):
            current_date = datetime.strptime(from_date, '%Y%m%d')
        else:
            current_date = from_date
        if isinstance(to_date, str):
            to_date = datetime.strptime(to_date, '%Y%m%d')
        # scan
        while current_date != to_date+one_day:
            if current_date.isoweekday() >= 6:
                current_date += one_day
                continue
            trades = TickDB.trade_list(security, current_date.strftime('%Y%m%d'))
            if len(trades.date) == 0:
                current_date += one_day
                continue
            pyLog("Getting volume curve on the " + current_date.strftime('%d/%m/%Y. %A'))
            trades_index = 0
            slice_time = datetime(current_date.year,
                                  current_date.month,
                                  current_date.day,
                                  trades.date[trades_index].hour, 
                                  0,
                                  0,
                                  0)
            volume_slices       = [0]
            while trades_index < len(trades.date):
                while trades.date[trades_index] > slice_time + timestep:
                    slice_time += timestep
                    volume_slices.append(0)
                volume_slices[-1] += trades.value['VOLUME'][trades_index]
                trades_index += 1
            if this_is_the_first_date:
                for i in range(len(volume_slices)):
                    key_str = 'vol_slice_%03d' %i
                    values[key_str] = []
            datetimes.append(current_date)
            for i in range(len(volume_slices)):
                key_str = 'vol_slice_%03d' %i
                values[key_str].append(volume_slices[i])
            this_is_the_first_date = False
            current_date += one_day
        return pyData('init', date=datetimes, value = values)
    
    @staticmethod
    def get_median_volume_slices(security, minutes_per_slice, from_date, to_date):
        # convert
        one_day                = timedelta(days=1)
        timestep               = timedelta(minutes=minutes_per_slice)
        this_is_the_first_date = True
        datetimes              = []
        values                 = []
        if isinstance(from_date, str):
            current_date = datetime.strptime(from_date, '%Y%m%d')
        else:
            current_date = from_date
        if isinstance(to_date, str):
            to_date = datetime.strptime(to_date, '%Y%m%d')
        # scan
        while current_date != to_date+one_day:
            if current_date.isoweekday() >= 6:
                current_date += one_day
                continue
            trades = TickDB.trade_list(security, current_date.strftime('%Y%m%d'))
            if len(trades.date) == 0:
                current_date += one_day
                continue
            pyLog("Getting volume curve on the " + current_date.strftime('%d/%m/%Y. %A'))
            trades_index = 0
            slice_time = datetime(current_date.year,
                                  current_date.month,
                                  current_date.day,
                                  trades.date[trades_index].hour, 
                                  0,
                                  0,
                                  0)
            volume_slices       = [0]
            while trades_index < len(trades.date):
                while trades.date[trades_index] > slice_time + timestep:
                    slice_time += timestep
                    volume_slices.append(0)
                    if this_is_the_first_date:
                        datetimes.append(slice_time)
                volume_slices[-1] += trades.value['VOLUME'][trades_index]
                trades_index += 1
            if this_is_the_first_date:
                for i in range(len(volume_slices)):
                    values.append([])
            for i in range(len(volume_slices)):
                values[i].append(volume_slices[i])
            this_is_the_first_date = False
            current_date += one_day
        # build median curve
        median_values = []
        for i in range(len(values)):
            values[i].sort()
            n = len(values[i])
            if n%2 == 1:
                median_values.append(values[i][n/2])
            else:
                median_values.append( (values[i][n/2]+values[i][(n-2)/2]) / 2 )
        return pyData('init', date=datetimes, value = {'1min_median_volume_slices':median_values})

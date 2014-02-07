# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 11:11:32 2013

@author: whuang
"""

# display size, auction dark no found 

import numpy as np

dict_para_mandatory = {'VWAP': ['OrderQty'],
                       'TWAP': ['OrderQty'],
                       'VOLUME': ['OrderQty','MaxPctVolume'],
                       'VOL': ['OrderQty','MaxPctVolume'],
                       'ICEBERG':['OrderQty','Price'],
                       'DYNAMIC VOLUME':['OrderQty','MinPctVolume','MaxPctVolume'],
                       'DYNVOL': ['OrderQty','MinPctVolume','MaxPctVolume'],
                       'IS': ['OrderQty'],
                       'CLOSE':['OrderQty'],
                       'HUNT':['OrderQty','Price'],
                       'CROSSFIRE':['OrderQty','Price'],
                       'BLINK':['OrderQty','Price']
                       }
                       
dict_para_optional = {'VWAP': ['Price','StartTime','EndTime','MaxPctVolume','ExecutionStyle','WouldLevel','WouldDark','AuctionPct','AggreggatedStyle','ExcludeAuction'],
                       'TWAP': ['Price','StartTime','EndTime','MaxPctVolume','ExecutionStyle','WouldLevel','WouldDark','AuctionPct','AggreggatedStyle','ExcludeAuction'],
                       'VOLUME': ['Price','StartTime','EndTime','MinPctVolume','ExecutionStyle','WouldLevel','WouldDark','AuctionPct','AggreggatedStyle','ExcludeAuction'],
                       'VOL': ['Price','StartTime','EndTime','MinPctVolume','ExecutionStyle','WouldLevel','WouldDark','AuctionPct','AggreggatedStyle','ExcludeAuction'],
                       'ICEBERG':['StartTime','EndTime','ExcludeAuction', 'MaxFloor'],
                       'DYNAMIC VOLUME': ['Price','StartTime','EndTime','BenchPrice','ExecutionStyle','WouldLevel','WouldDark','AuctionPct','AggreggatedStyle','ExcludeAuction'],
                       'DYNVOL': ['Price','StartTime','EndTime','BenchPrice','ExecutionStyle','WouldLevel','WouldDark','AuctionPct','AggreggatedStyle','ExcludeAuction'],
                       'IS': ['Price','StartTime','EndTime','BenchPrice','ExecutionStyle','WouldLevel','WouldDark','AggreggatedStyle','ExcludeAuction'],
                       'CLOSE':['Price','MaxPctVolume','ExecutionStyle','AuctionPct','AggreggatedStyle'],
                       'HUNT':['StartTime','EndTime','MinSize','AggreggatedStyle'],
                       'CROSSFIRE':['StartTime','EndTime','MinSize','SweepLit','OBType'],
                       'BLINK':['StartTime','EndTime','MinSize','OBType']
                       }
                       
def get_para_mandatory(strategy):
    return dict_para_mandatory[strategy]
    
def get_para_optional(strategy):
    return dict_para_optional[strategy]
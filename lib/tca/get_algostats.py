# -*- coding: utf-8 -*-
"""
Created on Tue Jun 11 11:27:13 2013

@author: njoseph
"""
import pandas as pd
import numpy as np
from datetime import *
import lib.stats.slicer as slicer
import lib.dbtools.get_algodata as get_algodata
import lib.dbtools.read_dataset as read_dataset

    
#--------------------------------------------------------------------------
# sequence_info
#--------------------------------------------------------------------------
def sequence(**kwargs):    
    ###########################################################################
    #### extract algo DATA
    ###########################################################################
    # get all the sequences from sequence ids
    if "sequence_id" in kwargs.keys():
        data=get_algodata.sequence_info(sequence_id=kwargs["sequence_id"])
    # get all the sequences from occurence ids
    elif "occurence_id" in kwargs.keys():  
        data=get_algodata.sequence_info(occurence_id=kwargs["occurence_id"])
    else:
        raise NameError('get_algostats:sequence - Bad inputs')
        
    if not data:
        return data
        
        
        
    
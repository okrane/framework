# -*- coding: utf-8 -*-
"""
Created on Fri May 24 16:04:51 2013

@author: njoseph
"""

import pandas as pd
import scipy.io
from datetime import *
import os as os
# from lib.dbtools.connections import Connections
from lib.dbtools.get_repository import *
from lib.data.matlabutils import *
from lib.data.st_data import *


def read_dataset(mode, **kwargs):
    
    #### CONFIG and CONNECT
    # TODO: in xml file
    ft_root_path="Q:\\kc_repository"
    # a trouver dans Connections
    
    #### DEFAULT OUTPUT    
    out=[]
    
    #--------------------------------------------------------------------------
    # MODE : FT 
    #--------------------------------------------------------------------------
    if (mode=="ft"):
        #---- security_info
        if "security_id" in kwargs.keys():  
            ids=kwargs["security_id"]
        else:
            raise NameError('read_dataset:ft - Bad input')
        #---- date info
        if "date" in kwargs.keys():
            date=kwargs["date"]
            date_newf=datetime.strftime(datetime.strptime(date, '%d/%m/%Y'),'%Y_%m_%d') 
        else:
            raise NameError('read_dataset:ft - Bad input')        
        
        #---- load and format
        filename=os.path.join(ft_root_path,'get_tick','ft','%d'%(ids),'%s.mat'%(date_newf))
        try:
            mat = scipy.io.loadmat(filename, struct_as_record  = False)
        except:
            raise NameError('read_dataset:ft - This file does not exist <'+filename+'>')      
        out=to_dataframe(mat['data'],timezone=True)
    #--------------------------------------------------------------------------
    # OTHERWISE
    #--------------------------------------------------------------------------
    else:
        raise NameError('read_dataset:mode - Unknown mode <'+mode+'>')
        
    return out
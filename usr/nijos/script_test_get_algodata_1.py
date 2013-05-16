# -*- coding: utf-8 -*-
"""
Created on Thu May 16 14:02:13 2013

@author: njoseph
"""
from lib.dbtools.get_algodata import *

# mode "sequenceinfo"
data=get_algodata("sequence_info",sequence_id="FY2000007382301")
data=get_algodata("sequence_info",sequence_id=["FY2000007382301","FY2000007414521"])
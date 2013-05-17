# -*- coding: utf-8 -*-
"""
Created on Thu May 16 14:02:13 2013

@author: njoseph
"""
from lib.dbtools.get_algodata import get_algodata

# mode "sequenceinfo"
#data=get_algodata("sequence_info",sequence_id="FY2000007381401")
#data=get_algodata("sequence_info",sequence_id=["FY2000007382301","FY2000007414521"])
#data=get_algodata("sequence_info",occurence_id="FY2000007383101")
data=get_algodata("sequence_info",start_date="14/05/2013",end_date="15/05/2013")
# mode "occurenceinfo"
data=get_algodata("occurence_info",occurence_id=["FY2000007383101"])
# mode deal
data=get_algodata("deal",sequence_id="FY2000004393001")

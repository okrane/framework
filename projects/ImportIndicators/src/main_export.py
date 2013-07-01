'''
Created on 1 Jul 2013

@author: flexsys
'''

from projects.ImportIndicators.src import ImportIndicators
import paramiko
from datetime import datetime
import os
import shutil
import subprocess
import socket

if __name__ == '__main__':
    
    # - PARAMETERS
    ip_server = '10.157.81.70'
    day = datetime.today().strftime('%Y%m%d')
    perimeter = 'all'
    l_indicator = ['1', '2']
    db = 'MARKET_DATA'
    hostname = socket.gethostbyname(socket.gethostname())
    
    print hostname
    
    # - CREATE TREE FOR EXPORTING FILES
    orig_path = '../%s/' %day
    path = '/home/flexsys/volume_curves/'
    
    if os.path.exists(orig_path):
        shutil.rmtree(orig_path)
    
    os.mkdir(orig_path)
    
    # - EXPORTING DATA INDICATORS + VOLUME CURVES FROM DATABASE
    ImportIndicators.ImportIndicators(db, l_indicator, perimeter, orig_path)
    
    
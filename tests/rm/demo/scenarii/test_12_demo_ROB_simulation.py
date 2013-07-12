# ------------------------------------------------- #
# Get Path                                          #
# ------------------------------------------------- #
import sys
import os.path
path = os.path.join(sys.argv[0].split('st_sim')[0],'st_sim')


# ------------------------------------------------- #
# Create the Scheduler : this is the SIMEP core. It #
# takes as input argument a path in order to store  #
# the log file of the simulation.                   #
# Trace is an object corrersponding to the log      #
# buffer.                                           #
# ------------------------------------------------- #
from simep.sched import Scheduler
sched = Scheduler('C:/st_sim/simep/Log')
trace = sched.getTrace()
# ------------------------------------------------- #



# ------------------------------------------------- #
# Create a stock model. Here we use the ZIM, which  #
# means "Zero Intelligence Model". If you want to   #
# have a look at the implementation of this model,  #
# please refer to the zimmodel.py file. Briefly,    #
# this model simply sends random limit orders, in   #
# other words : orders with random quantity,        #
# random price (but centered on 10$), and random    #
# side (buy or sell).                               #
# Basically, you just add an orderbook to the       #
# scheduler ('normal' is the type of orderbook,     #
# 'ACCPpa.CHI' is the name of the stock), then      #
# instantiate your model, then add your instance to #
# the scheduler.                                    #
# ------------------------------------------------- #
from simep.robmodel import ROBModel
sched.addOrderBook('light', 'ACCPpa.CHI')
stock_ACCP = ROBModel({'login'                  : 'a_lambda_guy', 
                       'name'                   : 'stock_ACCP'},
                      {'trading_destination_id' : 61, 
                       'name'                   : 'stock_ACCP', 
                       'trading_destination_name' : 'ENPA', 
                       'input_file_name'        : 'C:/st_sim/dev/tests/demo/data/lobTrade_2_61_20100524.binary', 
                       'date'                   : '20100524', 
                       'security_id'            : 2, 
                       'ric'                    : 'ACCPpa.CHI'},
                      {'number_of_bt'           : 10, 
                       'full'                   : False},
                      trace)
sched.addAgent(stock_ACCP)
# ------------------------------------------------- #



# ------------------------------------------------- #
# Run the simulation.                               #
# ------------------------------------------------- #
sched.run()
# ------------------------------------------------- #



# ------------------------------------------------- #
# Destroy objects.                                  #
# ------------------------------------------------- #
del(stock_ACCP)
del(trace)
del(sched)
# ------------------------------------------------- #




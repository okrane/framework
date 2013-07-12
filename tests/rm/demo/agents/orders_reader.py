from simep.sched import Order, Event
from simep.tools import date2num
from simep.agents.baseagent import BaseAgent



class OrdersReader(BaseAgent):
    
    @staticmethod
    def public_parameters():
        
        setup      = {'name'                   : {'label' : 'Name'               , 'value' : 'OrdersReader001'},
                      'engine_type'            : {'label' : 'Engine Type'        , 'value' : 'normal'}}
        parameters = {'start_time'             : {'label' : 'Start Time'         , 'value' : '07:00:00:000'}, 
                      'delta_time'             : {'label' : 'Delta Time'         , 'value' : '00:00:01:000'},
                      'log_filename'           : {'label' : 'Log Filename'       , 'value' : None}, 
                      'orders_filename'        : {'label' : 'Orders Filename'    , 'value' : 'C:/st_sim/dev/tests/demo/scenarii/test_01_demo_LMT_orders.txt'},
                      'print'                  : {'label' : 'Print Orberbook and Comments', 'value' : True}} 
        return {'setup': setup, 'parameters': parameters}
    
    # constructor
    def __init__(self, setup, context, params, trace):
        self._timestamp      = params['start_time']
        params['start_time'] = int(date2num(params['start_time']))
        params['delta_time'] = int(date2num(params['delta_time']))
        # initialize the base class (BaseAgent)
        BaseAgent.__init__(self, setup, context, params, trace)
        # indicate you want to have the execution reports, since you're sending orders
        self.needExecReportEvt = False        
        self.needAllEvts       = False
        # parameters
        self.setIsSimulator(True)
        self._order_number     = 0
        self._orders_list      = {}
        # open file
        if self['log_filename'] != None:
            self._log_file         = open(params['log_filename'],'w')
        self._orders_file      = open(params['orders_filename'],'r')
        self._simul_is_over    = False
        self.time2wakeup       = self['start_time']
    
    def initialize(self, market, bus):
        BaseAgent.initialize(self, market, bus)
        self._orderbook = self.market.findOrderBook(self['ric'])
        if self._orderbook is None:
            raise ValueError("%s orderbook could not be found ..." % self['ric'])
    
    def record_action(self, action_str):
        if self['log_filename'] != None:
            self._log_file.write('\n')
            self._log_file.write('----------------------------------------------------\n')
            self._log_file.write(action_str + '\n')
            self._log_file.write('----------------------------------------------------\n')
            self._log_file.write('\n')
        if self['print']:
            print ''
            print '----------------------------------------------------'
            print action_str
            print '----------------------------------------------------'
            print ''
    
    def record_orderbook(self, lob):
        if date2num(self._timestamp) > self['start_time']:
            lob_str  = '%s, BID1: %04d @ %04.01f' %(self._timestamp, lob.getBid(0).size, lob.getBid(0).price)
            lob_str += '    '
            lob_str += 'ASK1: %04d @ %04.01f' %(lob.getAsk(0).size, lob.getAsk(0).price)
            if self['log_filename'] != None:
                self._log_file.write(lob_str + '\n')
            if self['print']:
                print lob_str
            for i in range(1,5):
                lob_str  = '%s, BID%d: %04d @ %04.01f' %('            ', i+1, lob.getBid(i).size, lob.getBid(i).price)
                lob_str += '    '
                lob_str += 'ASK%d: %04d @ %04.01f' %(i+1, lob.getAsk(i).size, lob.getAsk(i).price)
                if self['log_filename'] != None:
                    self._log_file.write(lob_str + '\n')
                if self['print']:
                    print lob_str
            if self['print']:
                print ''
            if self['log_filename'] != None:
                self._log_file.write('\n')
    
    def modify_order(self, order_id, price, qty):
        order        = self._orders_list[order_id]
        order.remain = qty
        order.price  = price
        self._orderbook.processReplaceOrder(order_id, order)
    
    def create_an_order(self, side, price, qty, order_type, exec_type, stop_price=0.0, shown_qty=0, order_id=''):
        order                  = Order()
        self._order_number    += 1
        if order_id != '':
            order.orderId      = order_id
        else:
            order.orderId      = "%s$%i" % (self['name'], self._order_number)
        order.brokerId         = self.brokerId        
        order.side             = side
        order.price            = price
        order.stopPrice        = stop_price
        order.orderType        = order_type
        order.execType         = exec_type
        order.initialQty       = qty
        order.shownQty         = shown_qty if shown_qty != 0 else qty
        order.hiddenQty        = order.initialQty - order.shownQty
        order.remain           = order.shownQty
        order.minQty           = 0
        order.creationTimeMs   = self.time2wakeup
        order.expirationTimeMs = 0
        self._orderbook.processCreateOrder(order)
        self._orders_list[order.orderId] = order
        if side == Order.Buy:
            side_str = 'BUY '
        else:
            side_str = 'SELL'
        if order_type == Order.Limit:
            order_type_str = 'LIMIT        '
            stop_price_str = ''
        elif order_type == Order.Market:
            order_type_str = 'MARKET       '
            stop_price_str = ''
        elif order_type == Order.MarketToLimit:
            order_type_str = 'MARKET2LIMIT '
            stop_price_str = ''
        elif order_type == Order.Stop:
            order_type_str = 'STOP         '
            stop_price_str = ', stop_price=%04.01f' %stop_price
        elif order_type == Order.StopLimit:
            order_type_str = 'STOPLIMIT    '
            stop_price_str = ', stop_price=%04.01f' %stop_price
        else:
            order_type_str = 'UNKOWN       '
            stop_price_str = ''
        if exec_type == Order.DAY:
            order_type_str += 'DAY '
        elif exec_type == Order.GTD:
            order_type_str += 'GTD '
        elif exec_type == Order.GTC:
            order_type_str += 'GTC '
        elif exec_type == Order.FOK:
            order_type_str += 'FOK '
        elif exec_type == Order.IOC:
            order_type_str += 'IOC '
        else:
            order_type_str += '??? '
        if shown_qty != 0:
            iceb_str = 'iceb_qty=%03d' % shown_qty
        else:
            iceb_str = ''
        lob_str = '%s, %s: %s %04d @ %04.01f %s%s' % (self._timestamp, order_type_str, side_str, qty, price, stop_price_str, iceb_str)
        if self['log_filename'] != None:
            self._log_file.write(lob_str + '\n')
        if self['print']:
            print lob_str
        return order
    
    def initialize_orderbook(self):
        current_line = self._orders_file.readline().replace('\n','')
        while not "# CONTINUOUS AUCTION" in current_line:
            if len(current_line) == 0:
                pass
            elif current_line[0] != '#':
                cmd = 'self.create_an_order(' + current_line + ')'
                exec cmd
            else:
                self.record_action(current_line[2:])
            current_line = self._orders_file.readline().replace('\n','')
        self.record_action(current_line[2:])
    
    def send_next_order(self):
        current_line = self._orders_file.readline().replace('\n','')
        while True:
            if len(current_line) == 0:
                pass
            elif '# END' in current_line:
                self.record_action(current_line[2:])
                self._simul_is_over = True
                return
            elif current_line[:2] == '-C':
                cmd = 'self._orderbook.processCancelOrder(' + current_line[3:] + ')'
                exec cmd
                break
            elif current_line[:2] == '-M':
                cmd = 'self.modify_order(' + current_line[3:] + ')'
                exec cmd
                break
            elif current_line[:5] == 'Order':
                cmd = 'self.create_an_order(' + current_line + ')'
                exec cmd
                break
            elif current_line[0] == '#':
                self.record_action(current_line[2:])
            current_line = self._orders_file.readline().replace('\n','')
    
    def process(self, event):
        if (self.time2wakeup == self['start_time']):
            self.time2wakeup += self['delta_time']
            self.initialize_orderbook()
            return None
        if(self._simul_is_over):
            if self['log_filename'] != None:
                self._log_file.close()
            self._orders_file.close()
            return True
        if event.eventType == Event.IdleEvent:
            self.time2wakeup += self['delta_time']
            return False
        self._timestamp = event.getEvtTimeStr()
        self.record_orderbook(event.getLob())
        self.time2wakeup += self['delta_time']
        self.send_next_order()
        return None
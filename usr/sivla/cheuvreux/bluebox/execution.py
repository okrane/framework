'''
Created on Oct 7, 2010

@author: syarc
'''
from cheuvreux.fidessa import rebate
from cheuvreux.fidessa.audit_trail import AuditTrail

class PassiveExecution:

    def __init__(self, order_id):
        self.order_id = order_id
        self._order = None

        # Extra member
        self.destination = None
        self.end_time = None
        self.rebate = None

        self.final_bid, self.final_ask = None, None
        self.arrival_bid, self.arrival_ask = None, None
        self.first_fill_time, self.last_fill_time = None, None

        self.nb_child = None
    def load_execution_data(self, fidessa_db):
        self._order = fidessa_db.get_fidessa_order(self.order_id)
        if not self._order:
            raise Exception("Order not found")

        self.nb_child = fidessa_db.get_nb_child(self.order_id)

        self._trades = fidessa_db.get_trades(self.order_id)
        self.rebate = rebate.get_rebate_for_trade(self._trades, self._order.date)

    @staticmethod
    def parse_destination(service, executor):
        if service in ('SWEEP', 'POST'):
            if executor != '':
                return service + ' ' + executor
            return service
        elif service in ('BLUEBOX'):
            return executor
        elif service not in ('ORS'):
            return service
        else:
            return service


    def analyize_audit_trail(self, audit_trail):
        for line in audit_trail:
            if not self.arrival_bid or not self.arrival_ask:
                self.arrival_bid, self.arrival_ask = float(line[12]), float(line[13])

            if self.destination is None and line[1] in ('CHLD', 'AGYE'):
                self.destination = self.parse_destination(line[5], line[6])
            if line[1] == 'ASSN': #Order has been routed
                self.destination = self.parse_destination(line[5], line[6])


            if line[1] in ['RELE', 'ESOR', 'COMP']:
                self.end_time = line[2].split(" ")[1].replace(" -0400s", "")
                self.final_bid, self.final_ask = float(line[12]), float(line[13])
            elif line[1] in ['CEXE', 'EXEE']:
                if not self.first_fill_time:
                    self.first_fill_time = line[2].split(" ")[1].replace(" -0400s", "")
                self.last_fill_time = line[2].split(" ")[1].replace(" -0400s", "")
#            elif self.destination is None and line[1] in ('SPLI', 'ASSN', 'ASRT'):
#                self.destination = AuditTrail.parseDestination(line[9])

        if not self.arrival_bid or not self.arrival_ask:
            self.arrival_bid = self.arrival_ask = 0.0

        if not self.final_bid or not self.final_ask:
            self.final_bid, self.final_ask = self.arrival_bid, self.arrival_ask

    @staticmethod
    def db_name():
        return 'passive_orders'

    @staticmethod
    def db_fields():
        type = ['TEXT','TEXT PRIMARY KEY','TEXT','TEXT','TEXT','TEXT','INTEGER','INTEGER','REAL','REAL',
                'TEXT','TEXT','REAL','REAL','REAL','REAL',
                'TEXT','REAL', 'REAL', 'TEXT', 'TEXT', 'TEXT', 'TEXT', 'INTEGER']
        return map(lambda (name, type): '%s %s' % (name, type), zip(PassiveExecution.header(), type))

    @staticmethod
    def header():
        return ['date','order_id','side','code','start','end','quantity','done','gross_price','limit_price',
                'first_fill_time','last_fill_time','arrival_bid','arrival_ask','final_bid','final_ask',
                'destination','rebate', 'exec_quality', 'market_id', 'algo', 'parent_order_id', 'order_note_id', 'nb_child']

    @staticmethod
    def db_index():
        return None

    def get_fields(self):
        if not self._order.limit_price:
            limit_price = ''
        else:
            limit_price = self._order.limit_price

        fields = [self._order.date, self.order_id, self._order.buy_sell, self._order.code,
                  self._order.start, self.end_time,
                  self._order.quantity, self._order.done, self._order.gross_price,
                  limit_price, self.first_fill_time, self.last_fill_time,
                  self.arrival_bid, self.arrival_ask, self.final_bid, self.final_ask,
                  self.destination, self.rebate, self.execution_quality(),
                  self._order.market_id, self._order.algo, self._order.parent_order_id,
                  self._order.note_id, self.nb_child]

        return dict(zip(PassiveExecution.header(), fields))

    def __str__(self):
        return ','.join(self.get_fields().values())

    def execution_quality(self):
        order = self._order
        if order.isBuy:
            arrival_opposite, final_opposite = self.arrival_ask, self.final_ask
        else:
            arrival_opposite, final_opposite = self.arrival_bid, self.final_bid

        try:
            quality = order.quantity * arrival_opposite - (order.done * order.gross_price + order.left * final_opposite)
        except TypeError:
            print 'TypeError'

        if order.isBuy:
            return quality
        else:
            return -quality


'''
Created on Jul 7, 2010

@author: syarc

'''
from cheuvreux.fidessa import FIDESSA_BASE_DIR
from datetime import datetime, timedelta
import os
import re
import time
import gzip
from cheuvreux.utils.date import go_ahead

class AuditTrail(object):
    '''
        Helper class to deal with the audit trail psv file.
    '''
    def __init__(self, date):
        ''' Constructor

            @param data Audit trail date
        '''
        self.filename = AuditTrail.searchAuditFile(date)
        self._data = dict()
        self._orderIds = self._root_order_ids = None
        self._minDateTime = None

    def setMinTime(self, minDateTime):
        ''' Set the minimum datetime to load trail. '''
        self._minDateTime = minDateTime

    def valid(self):
        return os.path.exists(self.filename)

    def orderIds(self):
        '''
            Returns the list of orders_ids to load, if None, load everything
        '''
        return self._orderIds

    def setOrderIds(self, orderIds):
        ''' Set the list of order_ids to load. '''
        self._orderIds = orderIds

    def setRootOrderIds(self, order_ids):
        self._root_order_ids = order_ids

    def stock(self, orderId):
        ''' Returns the stock dealt by an order. '''
        return self._data[orderId][0][18]

    def getDateTime(string):
        ''' Build a datetime object from a string '''
        string = string.strip()
        last_index = string.rfind(' ')
        if last_index > 0:
            string = string[0:last_index]
        return datetime.strptime(string, "%Y%m%d %H:%M:%S.%f")
    getDateTime = staticmethod(getDateTime)

    def wait_for_done(date):
        ''' Wait for the done file for the given date. '''
        file = "done%s.txt" % (date + datetime.timedelta(days=1)).strftime('%Y%m%d')
        while True:
            if os.path.exists(file):
                return True
            time.sleep(1000)
    wait_for_done = staticmethod(wait_for_done)

    def getFastDateTime(string):
        ''' Build a datetime object from a string. '''

        string = string.strip()
        return datetime(int(string[0:4]), int(string[4:6]), int(string[6:8]), int(string[9:11]),
                          int(string[12:14]), int(string[15:17]), int(string[18:25]))
        #return datetime.strptime(string, "%Y%m%d %H:%M:%S.%f")
    getFastDateTime = staticmethod(getFastDateTime)


    def loadData(self, ignore_dma=True):
        '''
            Load file into memory

            @param ignore_dma: if TRUE, DMA order will not be loaded
        '''
        allOrders = self._orderIds is None and self._root_order_ids is None
        orderIds = self._orderIds if self._orderIds else []

        if self.filename.endswith('.gz'):
            fd = gzip.open(self.filename, 'rb')
        else:
            fd = open(self.filename,'r')

        it = fd.__iter__()
        # Read first line
        headers = it.next().strip().split('|')
        dma_index = headers.index('SPONSORED_ORDER')
        for line in it:
            line = line.strip()
            fields = line.split('|')

            if not ignore_dma or fields[dma_index] == 'N':
                # Check time limit
                if self._minDateTime is not None:
                    time = AuditTrail.getFastDateTime(fields[3])
                    if time < self._minDateTime:
                        continue

                if allOrders or fields[0] in orderIds or fields[1] in self._root_order_ids:
                    if fields[0] not in self._data:
                        self._data[fields[0]] = list()
                    self._data[fields[0]].append(fields[1:])

        fd.close()

    def __getitem__(self, args):
        if len(args) == 2:
            return self._data[args[0]][args[1]]
        else:
            return self._data[args]

    def __iter__(self):
        return self._data.__iter__()

    def __len__(self):
        return len(self._data)

    def __contains__(self, order_id):
        return order_id in self._data

    def searchAuditFile(date):
        '''
            Returns the audit trail file for a date.
        '''

        if type(date) is type(''):
            date = datetime.strptime(date, "%Y%m%d")

        str_date = date.strftime("%Y%m%d")
        # First try to look in the current directory
        # Mainly here for development purpose
        for dir in os.listdir(os.getcwd()):
            fullpath = os.path.join(os.getcwd(), dir)
            if date is not None and fullpath.find(str_date) == -1:
                continue
            if fullpath.find('AUDIT_TRAIL') > -1:
                return fullpath

        # Fidessa audit file for date d are stored under
        # a folder named "d+1"
        tmp_date = (date + timedelta(1)).strftime("%Y%m%d")
        filename = os.path.join(FIDESSA_BASE_DIR, tmp_date,
                               'AUDIT_TRAIL.%s.psv' % str_date)
        if os.path.exists(filename):
            return filename
        elif os.path.exists(filename + '.gz'):
            return filename + '.gz'

        # Previous lookup failed, look everywhere !
        for dir in os.listdir(FIDESSA_BASE_DIR):
            fullpath = os.path.join(FIDESSA_BASE_DIR, dir)
            if os.path.isfile(fullpath):
                continue

            for file in os.listdir(fullpath):
                    if file.find(str_date) == -1:
                        continue
                    if file.find('AUDIT_TRAIL') > -1:
                        return os.path.join(fullpath, file)
    searchAuditFile = staticmethod(searchAuditFile)

    def search_bluebox_file(date):
        if type(date) is type(''):
            date = datetime.strptime(date, "%Y%m%d")

        str_date = date.strftime("%Y%m%d")

        filename = 'BLUEBOX_ORDER_DETAILS.%s.psv' % str_date
        # Fidessa audit file for date d are stored under
        # a folder named "d+1"
        tmp_date = (date + timedelta(1)).strftime("%Y%m%d")
        filename = os.path.join(FIDESSA_BASE_DIR, tmp_date, filename)
        if os.path.exists(filename):
            return filename

        return AuditTrail.search_filename(filename)

    search_bluebox_file = staticmethod(search_bluebox_file)

    def is_done_file_present(date):
        if type(date) is type(''):
            date = datetime.strptime(date, '%Y%m%d')
        date = go_ahead(date, 1)
        pattern = re.compile('done%s.\d+.txt' % date.strftime('%Y%m%d'))
        done_file = 'done%s.txt' % date.strftime('%Y%m%d')
        for file in os.listdir(FIDESSA_BASE_DIR):
            if pattern.match(file) or file == done_file:
                return True

        for file in os.listdir(FIDESSA_BASE_DIR + "\\..\\"):
            if pattern.match(file) or file == done_file:
                return True

        return False
    is_done_file_present = staticmethod(is_done_file_present)

    def search_filename(filename):
        for dir in os.listdir(FIDESSA_BASE_DIR):
            fullpath = os.path.join(FIDESSA_BASE_DIR, dir)
            if os.path.isfile(fullpath):
                continue

            for file in os.listdir(fullpath):
                if file == filename:
                    return os.path.join(fullpath, file)
    search_filename = staticmethod(search_filename)


    def parseChildOrderId(field):
        orderId = re.split('Split .* as child order (.*)', field)[1]
        return orderId
    parseChildOrderId = staticmethod(parseChildOrderId)

    def parseFill(field):
        field = field.replace(',','')
        values = re.split('Entered fill (\(notified\) )*(buy|sell) (\d+) .* at ([\d\.]+).* with .* [^\[]* \[([^\]]+)\]', field)
        if len(values) <= 1:
            print field
            return None
        size, price, order_id = values[3:6]
        return float(size), float(price), order_id
    parseFill = staticmethod(parseFill)

    def parseDestination (destination):
        destination = re.split('to (.*) as', destination)[1]
        if destination.find(' on ') > 0:
            (executor, service) = re.split(' on ', destination)
            if service in ['SWEEP', 'BLUEBOX', 'POST'] :
                return executor
            else:
                return service
        return destination
    parseDestination = staticmethod(parseDestination)

    def parseRouteDestination(destination):
        destination = re.split('Routed to (.*)', destination)[1]
        if destination.find(' on ') > 0:
            (executor, service) = re.split(' on ', destination)
            if service.upper() in ['SWEEP', 'BLUEBOX', 'POST','NMS POST ORDERS'] :
                return executor
            else:
                return service
        return destination
    parseRouteDestination = staticmethod(parseRouteDestination)


if __name__ == '__main__':
    print AuditTrail.is_done_file_present(datetime(2012,3,2))

    field = 'Entered fill buy 100 EWS at 12.80USD with broker GFLO [00070029912TRCG0]'
    print AuditTrail.parseFill(field)
    field = 'Entered fill (notified) buy 2,722 BAC at 8.020USD with broker NYSE [00070080217TRCG0]'
    print AuditTrail.parseFill(field)
    field ='Entered fill (notified) sell 390 CTBG at 2.0USD with broker MAXM [00070014249TRCG0]'
    print AuditTrail.parseFill(field)
    field = 'Entered fill (notified) buy 700 MTZ at 17.910USD with broker GESX [00069994156TRCG0]'
    print AuditTrail.parseFill(field)

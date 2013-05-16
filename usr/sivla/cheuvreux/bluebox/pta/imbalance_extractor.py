'''
Created on Dec 7, 2011

@author: syarc
'''
from cheuvreux.stdio.ssh import SSHConfiguration
from cheuvreux.fidessa.audit_trail import AuditTrail
import os
import sys
import getopt
from cheuvreux.utils.date import parse_date, workday_range

def extract(dates):

    print 'date,parent_order_id,slice_order_id,time_stamp,aggressive_order'
    for date in dates:
        date = date.strftime('%Y%m%d')
        pool = SSHConfiguration('prod')
        client = pool.getClient('65.244.97.57')

        filename = 'parameters_CASE_PROD_FWK_2_%s.txt' % date
        try:
            client.get('/ext1/RFWK/runtime/parameters/%s' % filename, '.')
        except:
            continue

        slices = {}
        with open(filename, 'r') as file:
            for line in file:
                parent_order_id, imb, time = line.strip().split(',')[0:3]
                slices[imb.split('=')[1]] = [parent_order_id, time, None]

        with open(AuditTrail(date).filename, 'r') as file:
            for line in file:
                    if line.find('moved from passive destination') > 0:
                        fields = line.split('|')
                        order_id, event, field = fields[0], fields[2],fields[12]

                        if event == 'CHLD':
                            id = field.split("'")[1]

                            if id in slices:
                                slices[id][2] = order_id

        for slice_id in slices:
            parent_order_id, time, aggr_order_id = slices[slice_id]
            print ','.join([date,parent_order_id, slice_id, time, str(aggr_order_id)])

        os.remove(filename)

def usage(basename):
    print "Usage: %s [OPTIONS]" % basename;
    print ""
    print "Extract imbalance experiment data"
    print ""
    print " OPTIONS:"
    print "   -h, --help               \t Display this help message"
    print "   -d, --date=DATE          \t Date to process, format should"
    print "                            \t be YYYYMMDD, could be a range"

def main(argv):
    try:
        opts, args = getopt.getopt(argv[1:], 'hd:',
                                  ['help', 'date='])
    except getopt.GetoptError:
        usage(argv[0])
        sys.exit(-1)

    range = None

    for opt, val in opts:
        if opt in ("-h", "--help"):
            usage(argv[0])
            sys.exit(0)
        elif opt in ('-d', '--date'):
            if ':' in val:
                start, end = val.split(':')
                start, end = parse_date(start), parse_date(end)
                range = workday_range(start, end)
            else:
                start = parse_date(val)
                range = [start]

    if not range:
        usage(argv[0])
        sys.exit(-1)

    extract(range)

if __name__ == '__main__':
    main(sys.argv)

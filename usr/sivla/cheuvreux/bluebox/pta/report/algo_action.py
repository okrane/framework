'''
Created on Aug 19, 2011

@author: syarc
'''


from cheuvreux.fidessa.audit_trail import AuditTrail
from cheuvreux.stdio.mail import HtmlEmail
from cheuvreux.utils.dataset import Dataset, to_html, Quantity, Percentage
from collections import defaultdict
import csv
import datetime
import operator
import os
import sys
from cheuvreux.fidessa import get_directory_for_date
from matplotlib.dates import date2num
import gzip



class AlgoActions():

    def __init__(self):
        self._data = defaultdict(int)

    def add_action(self, algo, action):
        self._data[(algo,action)] += 1

    def output (self, nb, max=None):
        if nb < 1:
            i = 0
            total = self.total()
            for d in sorted(self._data.iteritems(), key=operator.itemgetter(1), reverse=True):
                if float(d[1]) / total < nb:
                    nb  = i

                i = i + 1

        if max:
            nb = min(nb, max)

        for data in sorted(self._data.iteritems(), key=operator.itemgetter(1), reverse=True)[0:nb]:
            print data[0][0], data[0][1], data[1]

    def total(self):
        return sum(self._data.values())

    def __len__ (self):
        return len(self._data)

    def html_output(self, nb, max=None):

        data = []
        total = self.total()

        if nb < 1:
            i = 0
            for d in sorted(self._data.iteritems(), key=operator.itemgetter(1), reverse=True):
                if float(d[1]) / total < nb:
                    nb  = i
                    break

                i = i + 1
        if max:
            nb = min(nb, max)

        if nb < 1:
            nb = len(self._data)

        for d in sorted(self._data.iteritems(), key=operator.itemgetter(1), reverse=True)[0:nb]:
            data.append([d[0][0], d[0][1], Quantity(d[1]), Percentage(float(d[1]) / total)])

        ds = Dataset(['Algo', 'Action', 'Count', 'Perc'], data)

        ds.add_total_row(['Total','',Quantity(total),''])

        ds.set_extra_style(['', '', "style='text-align:right;'"])
        ds.set_col_width([200, 200, 100, 100])
        return to_html(ds)

def report(date, output=sys.stdout, html=False):

    all_actions = AlgoActions()
    eod_actions = AlgoActions()

    filepath = os.path.join(get_directory_for_date(date), 'ALGO_ACTION_HISTORY.%s.psv' % date.strftime('%Y%m%d'))
    #filepath = 'ALGO_ACTION_HISTORY.20111116.psv'
    if not os.path.exists(filepath):
        if not os.path.exists(filepath + '.gz'):
            raise Exception("File doesn't exist " + filepath)
        else:
            filepath += '.gz'

    if filepath.endswith('.gz'):
        file = gzip.open(filepath,'rb')
    else:
        file = open(filepath,'r')
    reader = csv.DictReader(file, delimiter='|')

    eod_from = datetime.datetime(date.year, date.month, date.day, 15, 59, 0)
    eod_end = datetime.datetime(date.year, date.month, date.day, 16, 0, 0)

    timestamps = []
    latencies = []
    counts = defaultdict(int)

    for r in reader:
        if r['EXECUTOR_ID'] != '':
            try:
                time = getDatetime(r['TIMESTAMP'])
                if time.date() < date:
                    continue

                all_actions.add_action(r['EXECUTOR_ID'], r['ALGO_ACTION_TYPE'])

                event_time = getDatetime(r['ALGO_EVENT_DATETIME'])
                timestamps.append(time)
                diff = time - event_time
                latencies.append(diff.seconds + diff.microseconds / 1E6)
                counts[datetime.datetime(event_time.year, event_time.month, event_time.day, event_time.hour, event_time.minute, event_time.second)] += 1

                if time >= eod_from and time < eod_end:
                    if r['EXECUTOR_ID'] == 'CROSSFIRE' and r['ALGO_EVENT_TYPE'] == 'onMarketDataUpdate':
                        print r
                    eod_actions.add_action(r['EXECUTOR_ID'], r['ALGO_ACTION_TYPE'])

            except ValueError:
                print r

    file.close()

    filename = generate_chart(timestamps, latencies)
    filename2 = generate_count(counts)

    if html:
        if filename:
            print >> output, '<img src="cid:%s">' % filename
            output.attachImage(filename, filename)
        if filename2:
            print >> output, '<img src="cid:%s">' % filename2
            output.attachImage(filename2, filename2)

        print >> output, '<h2>All Actions</h2>'
        print >> output, all_actions.html_output(0.01,25)
        if len(eod_actions) > 0:
            print >> output, '<h2>EOD Actions</h2>'
            print >> output, eod_actions.html_output(0.01,25)

    else:
        print >> output, all_actions.output(25)
        print >> output, " ----------- "
        print >> output, eod_actions.output(25)


    slow_times = set()
    for i in range(0,len(timestamps)):
        if latencies[i] > 2 and timestamps[i] < eod_from:
            t = timestamps[i]

            a = datetime.datetime(t.year, t.month, t.day, t.hour, t.minute, 0) - datetime.timedelta(minutes=1)
            b = datetime.datetime(t.year, t.month, t.day, t.hour, t.minute, 0) + datetime.timedelta(minutes=1)
            slow_times.add((a,b))

    for (a,b) in sorted(slow_times,key=operator.itemgetter(0)):
        actions = action_by_range(a, b)
        if html:
            print >> output, '<h2>%s - %s</h2>' % (a.time(),b.time())
            print >> output, actions.html_output(0.05)
        else:
            print >> output, '%s - %s' % (a.time(),b.time())
            print >> output, actions.output(0.05)


def getDatetime(string):
    return datetime.datetime(int(string[0:4]), int(string[4:6]), int(string[6:8]), int(string[8:10]),
                          int(string[10:12]), int(string[12:14]), int(string[14:20]))

def action_by_range(start, end):
    filepath = os.path.join(get_directory_for_date(start.date()), 'ALGO_ACTION_HISTORY.%s.psv' % start.date().strftime('%Y%m%d'))
    #filepath = 'ALGO_ACTION_HISTORY.20111116.psv'
    if not os.path.exists(filepath):
        if not os.path.exists(filepath + '.gz'):
            raise Exception("File doesn't exist " + filepath)
        else:
            filepath += '.gz'

    if filepath.endswith('.gz'):
        fd = gzip.open(filepath, 'rb')
    else:
        fd = open(filepath,'r')

    reader = csv.DictReader(fd, delimiter='|')

    actions = AlgoActions()

    for r in reader:
        if r['EXECUTOR_ID'] != '':
            try:
                time = getDatetime(r['TIMESTAMP'])
                if time < start or time > end:
                    continue

                actions.add_action(r['EXECUTOR_ID'], r['ALGO_ACTION_TYPE'])

            except ValueError:
                print r

    return actions

def generate_count(counts):
    import matplotlib
    from matplotlib.pyplot import figure, show
    from matplotlib.dates import DateFormatter

    print len(counts)

    fig = figure()
    matplotlib.rcParams['font.size'] = 8.0
    ax = fig.add_subplot(111)
    ax.plot_date(counts.keys(),counts.values(), markersize=2)
    ax.set_title('Nb of actions per second')
    ax.set_ylabel('Nb of actions')
    ax.fmt_xdata = DateFormatter('%H:%M:%S')

    timestamp = counts.keys()[0]
    start = datetime.datetime(timestamp.year, timestamp.month, timestamp.day,7,0,0)
    end = datetime.datetime(timestamp.year, timestamp.month, timestamp.day,16,1,0)
    ax.set_xlim( date2num(start), date2num(end))

    fig.autofmt_xdate()
    fig.subplots_adjust(left=.05, bottom=.2, right=.95, top=.9)
    filename = 'action_count.png'
    fig.set_size_inches(10., 3.)

    fig.savefig(filename)
    return filename

def generate_chart(timestamps,latencies):
    import matplotlib
    from matplotlib.pyplot import figure, show
    from matplotlib.dates import DateFormatter

    fig = figure()
    matplotlib.rcParams['font.size'] = 8.0
    ax = fig.add_subplot(111)
    ax.plot_date(timestamps,latencies, markersize=2)
    ax.set_title('Events latency')
    ax.set_ylabel('Seconds')
    ax.fmt_xdata = DateFormatter('%H:%M:%S')

    start = datetime.datetime(timestamps[0].year, timestamps[0].month, timestamps[0].day,7,0,0)
    end = datetime.datetime(timestamps[0].year, timestamps[0].month, timestamps[0].day,16,1,0)
    ax.set_xlim( date2num(start), date2num(end))

    fig.autofmt_xdate()
    fig.subplots_adjust(left=.05, bottom=.2, right=.95, top=.9)
    filename = 'action_latency.png'
    fig.set_size_inches(10., 3.)

    fig.savefig(filename)
    return filename

if __name__ == '__main__':


    email = HtmlEmail('smtpnotes')
    email.set_subject('Algo Actions')
    email.set_dest('sarchenault@cheuvreux.com')
    report(datetime.date(2012, 1, 25), email, True)
    email.flush()




'''
Created on Oct 25, 2010

@author: syarc
'''

from cheuvreux.bluebox.pta import aggressive, passive, tca
from cheuvreux.stdio.mail import Email, HtmlEmail
from cheuvreux.utils.date import parse_date, previous_weekday, workday_range
import getopt
import sys
import traceback
import datetime
from cheuvreux.stdio.html import table_style
import os


TCA_OUTPUT_DIR = 'tca'

def pta(argv):

    force = False
    start, end, range = previous_weekday(), None, [previous_weekday()]
    output, error, email = sys.stdout, sys.stderr, False
    mode = None
    html_output = False

    try:
        opts, args = getopt.getopt(argv[1:], 'had:fe:pt',
                                  ['help', 'aggressive', 'date=', 'force',
                                   'email=','passive', 'tca'])
    except getopt.GetoptError:
        usage(argv[0])
        sys.exit(2)

    for opt, val in opts:
        if opt in ("-h", "--help"):
            usage(argv[0])
            sys.exit(0)
        elif opt in ('-f', '--force'):
            force = True
        elif opt in ('-d', '--date'):
            if ':' in val:
                start, end = val.split(':')
                start, end = parse_date(start), parse_date(end)
                range = workday_range(start, end)
            else:
                start = parse_date(val)
                range = [start]

        elif opt in ('-e', '--email'):
            output, error = HtmlEmail("smtpnotes", style=table_style), Email("smtpnotes")
            output.set_sender("Sylvain Archenault <sarchenault@cheuvreux.com>")
            error.set_sender("Sylvain Archenault <sarchenault@cheuvreux.com>")
            error.set_type('html')
            error.set_dest('sarchenault')
            output.set_dest(val)
            email = html_output = True


        # Commands
        elif opt in ('-a', '--aggressive'):
            mode = 'aggressive'
        elif opt in ('-p', '--passive'):
            mode = 'passive'
        elif opt in ('-t','--tca'):
            mode = 'tca'

    sys.stderr = open('log/log_%s.txt' % datetime.date.today().strftime('%Y%m%d'), 'a')

    if not mode:
        print >> sys.stderr, "A command should be specified\n"
        usage(argv[0])
        sys.exit(-1)

    try:
        if mode == 'aggressive':
            if email:
                output.set_subject("Price improvement report " + start.isoformat())
                if end is not None and end.isoformat() != start.isoformat():
                    output.set_subject(output.subject() + "-" + end.isoformat())

            aggressive.main(output, range, None, force, html_output)
        elif mode == 'passive':
            if email:
                output.set_subject("Passive order report " + start.isoformat())
                if end is not None and end.isoformat() != start.isoformat():
                    output.set_subject(output.subject() + "-" + end.isoformat())
            passive.main(output, range, None, force, html_output)

        elif mode == 'tca':
            for date in range:
                print date
                tca_output = open(os.path.join(TCA_OUTPUT_DIR,
                                               'tca_%s.csv' % date.strftime('%Y%m%d')),'w')
                tca.tca(date, tca_output)

                tca_output.close()

                tca.tca_to_sql(date, date)
        output.flush()
    except Exception:
        if html_output:
            print >> error, '<pre>'
        traceback.print_exc(file=error)
        if html_output:
            print >> error, '</pre>'
        error.flush()


def usage(basename):
    print "usage: %s [OPTIONS] [COMMANDS]" % basename;
    print ""
    print "COMMANDS:"
    print "  -h, --help         \t Print this help message"
    print "  -a, --aggressive   \t Aggressive order report (PI, Fill rate, cost, ...)"
    print "  -p, --passive      \t Passive order report (Fill rate, rebate, ...)"
    print "  -t, --tca          \t Run TCA report"
    print ""
    print "OPTIONS:"
    print "  -d, --date=<DATE>  \t Specify the date (default is last working day)"
    print "                     \t could be a range START:END (format is YYYYMMDD)"
    print "  -f, --force        \t Force the computation"
    print "  -e, --email=<DEST> \t Send the result via email to <DEST> (comma separated)"


if __name__ == '__main__':
    pta(sys.argv)

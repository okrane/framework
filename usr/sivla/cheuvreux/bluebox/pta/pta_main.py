'''
Created on Jun 30, 2011

@author: syarc
'''
import sys
import getopt
from cheuvreux.stdio.mail import Email, HtmlEmail
from cheuvreux.stdio.html import table_style
from cheuvreux.utils.date import workday_range, parse_date, previous_weekday
import traceback
from cheuvreux.bluebox.pta import loader, tca
from cheuvreux.bluebox.pta.report import midpoint, xfire_ping, algo_usage,\
    algo_action, daily_route_count, overall_activity, latency, xfire,\
    prospective, vwap_midpoint, dma_gtd
import os

TCA_OUTPUT_DIR = 'tca'

def pta_main(argv):

    try:
        opts, args = getopt.getopt(argv[1:], 'hd:fe:lmxabrtokspvg',
                                  ['help', 'date=', 'force',
                                   'email=', 'load', 'midpoint', 'xfire',
                                   'algo','bluebox','route','tca','overall',
                                   'ack_latency','heatseek','prospective','vwap_midpoint',
                                   'dma_gtd'])
    except getopt.GetoptError:
        usage(argv[0])
        sys.exit(2)


    force = False
    start, end, range = previous_weekday(), None, [previous_weekday()]
    output, error, email = sys.stdout, sys.stderr, False
    html_output = False
    # Commands
    latency_report = overall = tca_report = bluebox = load = midpoint_report = xfire_report = algo = route = prospective_slices = False
    vwap_midpoint_report = heatseek = False
    dma_gtd_report = False
    email_dest = ''

    for opt, val in opts:
        if opt in ("-h", "--help"):
            usage(argv[0])
            sys.exit(0)
        elif opt in ('-f', '--force'):
            force = True
        elif opt in ('-e', '--email'):
            email = html_output = True
            email_dest = val
        elif opt in ('-d', '--date'):
            if ':' in val:
                start, end = val.split(':')
                start, end = parse_date(start), parse_date(end)
                range = workday_range(start, end)
            else:
                start = parse_date(val)
                range = [start]

        # Commands
        elif opt in ('-l', '--load'):
            load = True
        elif opt in ('-m', '--midpoint'):
            midpoint_report = True
        elif opt in ('-x', '--xfire'):
            xfire_report = True
        elif opt in ('-a', '--algo'):
            algo = True
        elif opt in ('-b', '--bluebox'):
            bluebox = True
        elif opt in ('-r', '--route'):
            route = True
        elif opt in ('-t', '--tca'):
            tca_report = True
        elif opt in ('-o', '--overall'):
            overall = True
        elif opt in ('-k', '--ack_latency'):
            latency_report = True
        elif opt in ('-s', '--heatseek'):
            heatseek = True
        elif opt in ('-p', '--prospective'):
            prospective_slices = True
        elif opt in ('-v', '--vwap_midpoint'):
            vwap_midpoint_report = True
        elif opt in ('-g', '--dma_gtd'):
            dma_gtd_report = True

    if email:
        size = '10pt' if not algo else '8pt'
        output, error = HtmlEmail("smtpnotes", style=table_style, font_size=size), Email("smtpnotes")
        output.set_sender("Lionel Massoulard <lmassoulard-ext@cheuvreux.com>")
        error.set_sender("Lionel Massoulard <lmassoulard-ext@cheuvreux.com>")
        error.set_type('html')
        error.set_dest('lmassoulard-ext')
        output.set_dest(email_dest)
    try:
        if load:
            if email:
                output.set_subject('Load PTA data '  + start.isoformat())
                if end is not None and end.isoformat() != start.isoformat():
                        output.set_subject(output.subject() + "-" + end.isoformat())
                print >> output, '<pre>'
            loader.process_range(range, force, output)

            if email:
                print >> output, '</pre>'

        if midpoint_report:
            if email:
                output.set_subject("Mid Point Report " + start.isoformat())
                if end is not None and end.isoformat() != start.isoformat():
                    output.set_subject(output.subject() + "-" + end.isoformat())
            midpoint.run_report(range, output, html_output)

        if xfire_report:
            if email:
                output.set_subject("Crossfire Ping Report " + start.isoformat())
                if end is not None and end.isoformat() != start.isoformat():
                    output.set_subject(output.subject() + "-" + end.isoformat())
            xfire_ping.run_report(range, output, html_output)

        if algo:
            if email:
                output.set_subject("Algo monitoring " + start.isoformat())
                if end is not None and end.isoformat() != start.isoformat():
                    output.set_subject(output.subject() + "-" + end.isoformat())
            for date in range:
                algo_usage.run_report(date, output, force)

        if tca_report:
            for date in range:
                print date
                tca_output = open(os.path.join(TCA_OUTPUT_DIR,
                                               'tca_%s.csv' % date.strftime('%Y%m%d')),'w')
                tca.tca(date, tca_output)

                tca_output.close()

                tca.tca_to_sql(date, date)

        if latency_report:
            if email:
                output.set_subject('Ack Latency ' + start.isoformat())
                if end is not None and end.isoformat() != start.isoformat():
                    output.set_subject(output.subject() + '-' + end.isoformat())
            latency.run_report(range, output, html_output)

        if overall:
            if email:
                output.set_subject('Overall electronic activity ' + start.isoformat())
                if end is not None and end.isoformat() != start.isoformat():
                    output.set_subject(output.subject() + '-' + end.isoformat())
            overall_activity.report(range, output, html_output)

        if bluebox:
            if email:
                output.set_subject("Bluebox action " + start.isoformat())
            algo_action.report(range[-1], output, html_output)

        if route:
            if email:
                output.set_subject("Route Count " + start.isoformat())
                if end is not None and end.isoformat() != start.isoformat():
                    output.set_subject(output.subject() + "-" + end.isoformat())
            daily_route_count.getFlows(start, end, output, html_output)

        if heatseek:
            if email:
                output.set_subject("Crossfire Heatseek " + start.isoformat())
                if end is not None and end.isoformat() != start.isoformat():
                    output.set_subject(output.subject() + "-" + end.isoformat())
            xfire.run_report(range, output, html_output)

        if prospective_slices:
            if email:
                output.set_subject('Prospective slices ' + start.isoformat())
                if end is not None and end.isoformat() != start.isoformat():
                    output.set_subject(output.subject() + "-" + end.isoformat())
            prospective.report(range, output)

        if vwap_midpoint_report:
            if email:
                output.set_subject('VWAP MidPoint posting ' + start.isoformat())
                if end and end.isoformat() != start.isoformat():
                    output.set_subject(output.subject() + "-" + end.isoformat())
            vwap_midpoint.report(range, output, html_output)
            
        if dma_gtd_report:
            if email:
                output.set_subject('DMA GTD order report ' + start.isoformat())
                if end and end.isoformat() != start.isoformat():
                    output.set_subject(output.subject() + "-" + end.isoformat())
            dma_gtd.report(start, end, output)
            
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
    print "  -l, --load         \t Load data in the database"
    print "  -m, --midpoint     \t Run Mid Point Report"
    print "  -x, --xfire        \t Crossfire Ping Report"
    print "  -s, --heatseek     \t Crossfure heatseek report"
    print "  -a, --algo         \t Algo Monitoring Report"
    print "  -b, --bluebox      \t Bluebox Action history"
    print "  -r, --route        \t Route count for a given time range"
    print "  -t, --tca          \t Compute TCA report"
    print "  -o, --overall      \t Overall electronic activity"
    print "  -k, --ack_latency  \t Ack latency report"
    print "  -p, --prospective  \t Prospective slice report"
    print "  -v, --vwap_midpoint\t VWAP Midpoint posting report"
    print "  -g, --dma_gtd      \t Run report to show GTD order on the DMA system"
    print ""
    print "OPTIONS:"
    print "  -d, --date=<DATE>  \t Specify the date (default is last working day)"
    print "                     \t could be a range START:END (format is YYYYMMDD)"
    print "  -f, --force        \t Force the computation"
    print "  -e, --email=<DEST> \t Send the result via email to <DEST> (comma separated)"

if __name__ == '__main__':
    pta_main(sys.argv)
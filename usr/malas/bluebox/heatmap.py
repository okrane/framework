'''
Created on Jul 13, 2010

@author: malas
'''

from collections import defaultdict
from datetime import date, timedelta
from usr.dev.sivla.funcs.DBTools.connections import Connections
from usr.dev.sivla.funcs.DBTools.repository import Repository, DictionaryId
from usr.dev.sivla.funcs.DBTools.security import Security
from usr.dev.sivla.funcs.DBTools.tickdb import TickDB
from simep.funcs.stdio.html import HtmlTable
from simep.funcs.stdio.mail import Email
from simep.funcs.utils.date import previous_weekday
import getopt
import locale
import math
import operator
import os
import shutil
import sys
import zipfile

locale.setlocale(locale.LC_ALL, '')

OUTPUT_DIR = 'output'
INPUT_DIR = 'heatmap2'

def loadIsinInDir(dir):
    '''
        Look for all files in a directory and returns
        filenames (without extension) as a set
    '''
    isins = set()
    i = 0
    for file in os.listdir(dir):
        if os.path.isfile(os.path.join(dir, file)) and file.endswith(".csv"):
            isins.add(file.split(".")[0])
            i += 1
            if i > 50000000:
                break
    return isins

class VolumeAggregator(object):
    '''
        Helper class to aggregate volume by destination and by date
        in order to generate heatmap files
    '''

    def __init__(self, sec_id):
        self.sec_id = sec_id
        # Data structure to store volumes on different dates / destination
        self.datas = defaultdict(dict)

    def addVolume(self, destination, volume, date):
        '''
            Add a new volume to the aggregator
        '''
        self.datas[str(date)][destination] = volume

    def generateHeatmapFile(self, isins, destination_map, output):
        '''
            Create the heatmap file

            isins           -- Map betweem isin and security ids
            destination_map -- Map between trading destination id and real exchang name
            output          -- Output directory
        '''
        if len(self.datas) <= 5:
            raise RuntimeWarning("Not enough data for " + isins[self.sec_id])

        # 1/ Compute daily weight for each day
        daily_weight = defaultdict(list)
        for date in self.datas:
            # Data for a given date
            data = self.datas[date]
            total = float(sum(volume for volume in data.values()))
            for destination in self.datas[date]:
                daily_weight[destination].append(data[destination] / total)


        # 2/ Compute average weight on the period
        probas = dict(map(lambda item: (item[0], 0.8 * sum(item[1]) / len(self.datas)),
                           daily_weight.items()))

        # FLOW Hack
        probas[666] = 0.2

        if os.path.exists(os.path.join(output, isins[self.sec_id] + ".csv")):
            print isins[self.sec_id]

        file = open(os.path.join(output, isins[self.sec_id] + ".csv"), "w")
        tmp_sum = 0
        for dest_id, proba in sorted(probas.iteritems(),
                                     key=operator.itemgetter(1),
                                     reverse=True):
            tmp_sum += proba
            try :
                print >> file, "%s,%.6f," % (destination_map[dest_id], tmp_sum)
            except KeyError:
                print self.sec_id

class HeatmapGenerator(object):
    '''
        Helper class to generate heatmap files
    '''


    td_ids = { 100 : 'BATS',
               99 : 'ARCX',
               105 : 'NSDQ', 95 : 'NSDQ', 94 : 'NSDQ', 93 : 'NSDQ',
               25 : 'NYSE',
               666 : 'FLOW',
               -1 : 'EDGX'
            }

    def __init__(self, in_dir):
        '''
            Constructor

            in_dir -- Directory containing the list of heatmap to update
        '''
        self.loadUniverse(in_dir)

    def loadUniverse(self, dir):
        '''
            Load security ids from the directory (internal method)
        '''

        self.isins = loadIsinInDir(dir)
        self.sec_ids = Repository.dict_id_to_security_id(self.isins, DictionaryId.ISIN)

    def loadVolumes(self, start, end):
        '''
            Retrieve volumes from tick_db.

            start, end -- Time window
        '''

        query = ''' select date,trading_destination_id,security_id,volume
                    from tick_db..trading_daily_ameri tb
                    where security_id in (%s) and date between '%s' and '%s'
                    and trading_destination_id in (%s) order by date, security_id
                ''' % (','.join(map(str, self.sec_ids.keys())), start, end,
                       ','.join(map(str, HeatmapGenerator.td_ids.keys())))

        cursor = Connections.getCursor('BSIRIUS')

        cursor.execute(query)
        rows = cursor.fetchall()

        self.volumes = {}
        for row in rows:
            if row[2] not in self.volumes:
                volume = VolumeAggregator(row[2])
                self.volumes[row[2]] = volume
            self.volumes[row[2]].addVolume(row[1], row[3], row[0])

    def generateFiles(self, output, stdout):
        '''
            Generate heatmap files

            output -- Output directory
            stdout -- File to output errors/warnings (should be writable object, i.e
                      implement write() method)
        '''

        init = False
        for volume in self.volumes.values():
            try:
                volume.generateHeatmapFile(self.sec_ids, HeatmapGenerator.td_ids, output)
            except RuntimeWarning as e:
                if not init:
                    print >> stdout, "<h2>Warnings</h2>"
                    init = True

                print >> stdout, e
        if init:
            print >> stdout, "<br>&nbsp;"

class Heatmap(object):
    '''
        Python object for heatmap files
    '''

    def __init__(self, file):
        '''
            Create a new heatmap based on a file
        '''

        self.weights = {}
        file = open(file, "r")
        previous_weight = 0.0
        for line in file:
            dest, weight = line.split(',')[0:2]
            self.weights[dest] = float(weight) - previous_weight
            previous_weight = float(weight)

    def distance(self, other):
        '''
            Compute the distance between two heatmap

            Distance is defined as :
                sqrt (sum ( (self.weight[destination] - other.weight[destination])^2 ) )

            if weight[destination] does not exist, 0 is assumed
        '''

        destination = set(self.weights.keys()).union(set(other.weights.keys()))

        distance = 0.0
        for dest in destination:
            if dest in other.weights and dest in self.weights:
                distance += (self.weights[dest] - other.weights[dest]) ** 2
            elif dest in self.weights:
                distance += self.weights[dest] ** 2
            elif dest in other.weights:
                distance += other.weights[dest] ** 2


        return math.sqrt(distance / len(destination))


    def check_values(self):
        sum = 0
        for dest in self.weights:
            if dest not in HeatmapGenerator.td_ids.values() and dest != 'EDGA':
                raise ValueError("Unknown destination: %s " % (dest))

            if self.weights[dest] < 0:
                raise ValueError("Inconsistent value (%f)" % (self.weights[dest]))
            sum += self.weights[dest]

        if abs(sum - 1.0) > 0.00000000001:
            raise ValueError("sum is not equal to 1.0 [%.10f]" % abs(sum - 1.0))


class HeatmapCheck(object):
    '''
        This class is in charge of running checks
        on heatmap files
    '''
    def __init__(self, input_dir, output_dir, stdout):
        '''
            Constructor

            input_dir  -- Directory where reference heatmaps are stored
            output_dir --  Directory where new heatmaps are stored
            stdout     -- Writable object used for reporting

        '''
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.stdout = stdout

        self.old_isins = loadIsinInDir(input_dir)
        self.new_isins = loadIsinInDir(output_dir)

        self.isins = self.old_isins.intersection(self.new_isins)

    def check_missing(self):
        '''
            Check if some heatmaps are missing in the new directory
        '''

        print >> self.stdout, "<h2>Missings</h2>"
        for isin in self.old_isins:
            if isin not in self.new_isins:
                print >> self.stdout, isin
        print >> self.stdout, "<br>&nbsp;"

    def check_distance(self, nb=50):
        '''
            Check heatmap distance.

            It outputs "nb" worse heatmaps.
        '''

        # compute distance
        qualities = []
        for isin in self.isins:
            old = Heatmap(os.path.join(self.input_dir, isin + ".csv"))
            new = Heatmap(os.path.join(self.output_dir, isin + ".csv"))
            qualities.append((isin, old.distance(new)))

        table = HtmlTable()
        table.setBorder()
        print >> self.stdout, "<h2>Quality checks</h2>"
        print >> self.stdout, table.header(['ISIN', 'Ticker', 'Distance', 'ADV'],
                                           width=[100])

        for isin, quality in sorted(qualities,
                                   key=operator.itemgetter(1),
                                   reverse=True)[:nb]:
            sec_id = Repository.dict_id_to_security_id(isin, DictionaryId.ISIN).keys()[0]
            ticker = Repository.security_dict_code(sec_id, DictionaryId.USCODE)
            volume = TickDB.volume(Security(sec_id, None),
                                   previous_weekday().strftime('%Y%m%d'))
            if volume is None:
                volume = 0
            print >> self.stdout, table.line([isin, ticker, "%.4f" % quality,
                                              locale.format("%d", volume, grouping=True)])

        print >> self.stdout, table.end()
        print >> self.stdout, "<br>&nbsp;"

def generateFiles(output, mail):
    today = date.today()

    if os.path.exists(output):
        shutil.rmtree(output)
    os.makedirs(output)

    generator = HeatmapGenerator(INPUT_DIR)
    generator.loadVolumes(date.strftime(today - timedelta(28), '%Y%m%d'), date.strftime(today, '%Y%m%d'))
    generator.generateFiles(output, mail)

    return output

def run_checks(output_dir, mail):
    check = HeatmapCheck(INPUT_DIR, output_dir, mail)
    #check.check_missing()
    check.check_distance()


def compressHeatmapDirectory(directory):
    zip = zipfile.ZipFile(os.path.basename(directory) + '.zip', 'w', zipfile.ZIP_DEFLATED)
    try:
        for file in os.listdir(directory):
            if os.path.isfile(os.path.join(directory, file)):
                zip.write(os.path.join(directory, file))
    finally:
        zip.close()

def main(argv):

    recipients = "sarchenault@cheuvreux.com"
    check_only, generation_only = False, False

    output_dir = "%s_%s" % (OUTPUT_DIR, date.strftime(date.today(), "%Y%m%d"))

    try:
        opts, args = getopt.getopt(argv[1:], 'gce:',
                                  ['generate', 'checks', 'email='])
    except getopt.GetoptError:
        sys.exit(2)

    for opt, val in opts:
        if opt in ("-e", "--email"):
            recipients = val
        elif opt in ("-c", "--checks"):
            check_only = True
        elif opt in ("-g", "--generate"):
            generation_only = True


    mail = Email("smtpnotes")
    mail.sender = "sarchenault@cheuvreux.com"
    mail.type = 'html'
    mail.subject = 'Heatmap Check Report'


    if not check_only:
        generateFiles(output_dir, mail)
        compressHeatmapDirectory(output_dir)
    if not generation_only:
        run_checks(output_dir, mail)

    mail.send(recipients)


if __name__ == '__main__':
    #main(sys.argv)
    isins = loadIsinInDir('G:\Research_Quant\Heatmap\heatmap')
    for isin in isins:
        h = Heatmap(os.path.join('G:\Research_Quant\Heatmap\heatmap', isin + '.csv'))
        try:
            h.check_values()
        except Exception as e:
            print isin, e

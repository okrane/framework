'''
Created on Jan 25, 2011

@author: syarc
'''
from cheuvreux.stdio.html import numberFormat
import locale

class Formatter:

    def __init__(self):
        pass

    def format(self):
        pass

class DefaultFormatter:

    def format(self, value):
        return value

class MoneyFormatter:
    def __init__(self):
        pass

    def format(self, value):
        return '$' + locale.format('%.02f', value, grouping=True)

class NumberFormatter:
    def __init__(self, format='%.2f', grouping=False):
        self._format = format
        self.grouping = grouping

    def format(self, value):
        return numberFormat(self._format, value, self.grouping)


class PercentageFormatter(NumberFormatter):

    def __init__(self):
        NumberFormatter.__init__(self, '%.2f%%', grouping=False)

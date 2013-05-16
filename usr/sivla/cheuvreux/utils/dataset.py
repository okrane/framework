'''
Created on Jun 27, 2011

@author: syarc
'''
from cheuvreux.stdio.html import HtmlTable, numberFormat
import locale


class Integer:
    def __init__ (self, value, ):
        if not value:
            value = 0

        self.value = int(value)

    def __str__(self):
        return '%d' % self.value

    def __int__(self):
        return self.value

    def __float__(self):
        return float(self.value)

    def __lt__(self, other):
        return self.value < int(other)

    def __le__(self, other):
        return self.value <= int(other)

    def __gt__(self, other):
        return self.value > int(other)

    def __ge__(self, other):
        return self.value >= int(other)

    def __radd__(self, other):
        return int(other) + self.value

    def __trunc__(self):
        return self.value

    html = __str__

class Float:
    def __init__ (self, value, precision=2):
        if not value:
            value = 0.0

        self.value = float(value)
        self.precision = precision

    def __str__(self):
        return ('%%.%df' % self.precision) % self.value

    def __float__(self):
        return self.value

    def __lt__(self, other):
        return self.value < float(other)

    def __le__(self, other):
        return self.value <= float(other)

    def __gt__(self, other):
        return self.value > float(other)

    def __ge__(self, other):
        return self.value >= float(other)

    def __radd__(self, other):
        return float(other) + self.value

    html = __str__

class Quantity:
    def __init__(self, value, unusual_function = None):
        self.value = int(value)
        self.unusual = unusual_function

    def __str__(self):
        return str(self.value)

    def __float__(self):
        return float(self.value)

    def __int__(self):
        return self.value

    def __lt__(self, other):
        return self.value < int(other)

    def __le__(self, other):
        return self.value <= int(other)

    def __gt__(self, other):
        return self.value > int(other)

    def __ge__(self, other):
        return self.value >= int(other)

    def __radd__(self, other):
        return float(other) + self.value

    def html(self):
        str = locale.format("%d",self.value, grouping=True)

        if self.unusual and self.unusual(self.value):
            str = unusual_html(str)

        return str

class Percentage(Float):
    def __init__(self, value, color=False):
        self.value = float(value)
        self.color = color

    def __str__(self):
        return '%.2f%%' % (100.0 * self.value)

    def html(self):
        if self.color:
            return numberFormat('%.2f%%', 100.0 * self.value)
        else:
            return str(self)


class BPs (Float):
    def __init__(self, value, color=False):
        self.value = 10000.0 * value
        self.color = color

    def __str__(self):
        return '%.2f' % self.value

    def html(self):
        if self.color:
            return numberFormat('%.2f', self.value)
        else:
            return str(self)

class ComposedValue():
    def __init__(self, main, other, inversed = False, threshold=0.2):
        self.main = main
        self.other = other

        self.threshold = threshold
        if not inversed:
            self.pos_color,self.neg_color = '#008000', '#FF0000'
        else:
            self.pos_color,self.neg_color = '#FF0000', '#008000'

    def __str__(self):
        if str(self.main) != str(self.other):
            return '%s (%s)' % (str(self.main), str(self.other))
        else:
            return str(self.main)

    def html(self):
        if str(self.main) != str(self.other):
            color = None
            if self.other.value > 0:
                if float(self.main.value) / self.other.value - 1 > self.threshold:
                    color = self.pos_color
                if float(self.other.value) > 0 and self.main.value / self.other.value - 1 < - self.threshold:
                    color = self.neg_color
            if color:
                value_str = '<span style="color: %s">%s</span>' % (color, self.main.html())
            else:
                value_str = str(self.main)

            return '%s <i><span style="font-size: 8pt">(%s)</span></i>' % (value_str, self.other.html())
        else:
            return self.main.html()

    def __float__(self):
        return float(self.main)

    def __int__(self):
        return int(self.main)

    def __lt__(self, other):
        return self.main < float(other)

    def __le__(self, other):
        return self.main <= float(other)

    def __gt__(self, other):
        return self.main > float(other)

    def __ge__(self, other):
        return self.main >= float(other)

    def __radd__(self, other):
        return float(other) + self.main

class ComposedFloat(Float):
    def __init__(self, main, other):
        Float.__init__(self,main.value,2)
        self.main = main
        self.other = other

    def __str__(self):
        return '%s (%s)' % (str(self.main), str(self.other))

    def html(self):
        color = None
        if self.other.value > 0 and self.main.value / self.other.value - 1 > 0.2:
            color = '#008000'
        elif self.other.value > 0 and self.main.value / self.other.value - 1 < -0.2:
            color = '#FF0000'

        if color:
            value_str = '<span style="color: %s">%s</span>' % (color, str(self.main))
        else:
            value_str = str(self.main)

        return '%s <i><span style="font-size: 8pt">(%s)</span></i>' % (value_str, self.other.html())

class Dollar(Float):

    def __init__(self, value, precision=2, color=False):
        Float.__init__(self, value, precision)
        self._color = color

    def __format(self):
        return '%%.%df' % self.precision

    def __str__(self):
        return '$' + self.__format() % self.value

    def html(self):
        str = locale.format(self.__format(), self.value, True)
        if self._color:
            if self.value > 0:
                return '<span style="color: #FF0000">$%s</span>' % (str)
            elif self.value <0:
                return '<span style="color: #008000">$(%s)</span>' % (str.replace('-',''))
                
            
        return '$' + str

class Dataset:
    def __init__ (self, header, data=None):
        self.header = header
        self.data = data if data else []
        self.style = ['']
        self.width = [100]
        self.total = None

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return self.data.__iter__()

    def __contains__(self, value):
        return value in self.header

    def set_extra_style(self, style):
        self.style = style

    def set_col_width(self, width):
        self.width = width

    def append(self, row):
        self.data.append(row)

    def __str__(self):
        result = '\t'.join(map(str,self.header)) + '\n'
        for line in self.data:
            result += '\t'.join(map(str,line)) + '\n'

        if self.total:
            result += '\t'.join(map(str, self.total)) + '\n'

        return result

    def sort(self, key, reverse=False):
        idx = self.header.index(key)
        self.data.sort(key= lambda x: x[idx], reverse=reverse)

    def add_total_row(self, total):
        self.total = total

    def col(self, key):
        if type (key) == type(''):
            idx = self.header.index(key)
        else:
            idx = int(key)
        return [line[idx] for line in self.data]


def to_html(ds):
    table = HtmlTable()
    table.setBorder()

    result = table.header(ds.header, width=ds.width)

    for line in ds:
        result += table.line(line, ds.style)

    if ds.total:
        result += table.line(['<b>%s</b>' % (t.html() if hasattr(t,'html') else str(t)) for t in ds.total], ds.style)

    result += table.end()

    return result


def unusual_html(str):
    return '<span style="color:#FF0000;font-weight:bold">%s</span>' % str
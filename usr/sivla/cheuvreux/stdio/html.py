'''
Created on 6 mai 2010

@author: syarc
'''
import locale

class HtmlTable:
    def __init__(self):
        self.border = 0

    def setBorder(self):
        self.border = 1

    def header(self, header, width=None):
        if not width:
            width = [100]

        #if not width or len(width) == 1:
        #    width = " width='%d'" % width if width is not None else ''
        #    return ''.join([self.style(), "<table border='%d'>\n" % self.border,
        #                   self.line(list, "style='text-align:center;font-weight:bold' %s" % width,
        #                             tag="th")])
        #else:
        return ''.join(["<table style='border-width:0px'>\n",
                        self.line(header,
                                  ["style='text-align:center;font-weight:bold' width='%d'" % w for w in width],
                                  tag="th")])

    def end(self):
        return "</table>\n"

    def line (self, data, extra='', tag="td"):
        str_list = ["<tr>\n"]
        for index, elem in enumerate(data):
            tmp_extra = extra
            if type(extra) is list:
                if index < len(extra):
                    tmp_extra = extra[index]
                else:
                    tmp_extra = extra[-1]

            if hasattr(elem,'html'):
                str_list.append("<%s %s>%s</%s>\n" % (tag, tmp_extra, elem.html(), tag))
            else:
                str_list.append("<%s %s>%s</%s>\n" % (tag, tmp_extra, str(elem), tag))
        str_list.append("</tr>\n")

        return ''.join(str_list)

def numberFormat(format, value, grouping=False):
    if grouping:
        string = locale.format(format, value, grouping=True)
    else:
        string = format % value

    if value > 0:
        ans = '<span style="color: #008000">%s</span>' % string
    elif value < 0:
        ans = '<span style="color: #FF0000">%s</span>' % string
    else:
        ans= '%s' % string

    return ans

def makeTable(output, header, data, width=[100]):
    table = HtmlTable()
    table.setBorder()
    print >> output, table.header(header, width)
    for line in data:
        print >> output, table.line(line)
    print >> output, table.end()


def table_style(font_size='10pt'):
    return '''
    <style type="text/css">
     <!-- th { font-size:%s; background-color: #009661; color:#FFFFFF; border:1px solid #7f6a50} -->
     <!-- td { font-size:%s; border:1px solid #7f6a50; font-family:Arial} -->
     <!-- table { border-width: 0px; border-collapse: collapse;} -->
    </style>''' % (font_size, font_size)

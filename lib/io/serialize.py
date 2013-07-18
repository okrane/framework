import simplejson
import datetime

class DateTimeJSONEncoder(simplejson.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            print obj
            return obj.isoformat() #.strftime("%y/%m/%d %H:%M:%S")
        else:
            return super(DateTimeJSONEncoder, self).default(obj)

import dateutil.parser
def as_datetime(dct):
    for el in dct:
        try:
            if len(dct[el]) > 7 :
                d = dateutil.parser.parse(dct[el])
                dct[el] = d
        except:
            pass
    return dct

# How to:
# --- ENCODE
# dictStr = DateTimeJSONEncoder().encode(dict)
# 
# f = open('file', 'w')
# f.write(dictStr)
# f.close()
# --- DECODE
# f = open('file', 'r')
# u_orders = simplejson.loads(f.read(), object_hook = as_datetime)
# f.close()

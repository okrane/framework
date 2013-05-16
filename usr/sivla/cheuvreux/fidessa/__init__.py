'''
    This package contains a collection of tools to help with the manipulation of
    Fidessa data.

    It could be the Fidessa database (Fidessadb module) or the fidessa psv files.
'''
import os
from datetime import datetime, timedelta

# Fidessa base directory for flat files
FIDESSA_BASE_DIR = "G:\\Fidessa_report\\hightouch"
if not os.path.exists(FIDESSA_BASE_DIR):
    FIDESSA_BASE_DIR = "F:\\Fidessa_report\\hightouch"
    if not os.path.exists(FIDESSA_BASE_DIR):
        FIDESSA_BASE_DIR = "C:\\Fidessa\\hightouch"
        

def get_directory_for_date(date):
    '''
        Returns the fidessa directory for a date
    '''
    # Fidessa audit file for date d are stored under
    # a folder named "d+1"
    if type(date) is type(''):
        tmp_date = datetime.strptime(date, "%Y%m%d")
    else:
        tmp_date = date

    if tmp_date.weekday() == 4:
        tmp_date = (tmp_date + timedelta(3))
    else:
        tmp_date = (tmp_date + timedelta(1))

    tmp_date = tmp_date.strftime("%Y%m%d")
    return os.path.join(FIDESSA_BASE_DIR, tmp_date)
    #return 'C:\\Temp\\Fidessa'


def find_file(file_pattern, date):
    base_dir = get_directory_for_date(date)
    str_date = date if type(date) == type('') else date.strftime('%Y%m%d')
    filename = os.path.join(base_dir, file_pattern % str_date)
    if os.path.exists(filename):
        return filename
    elif os.path.exists(filename + '.gz'):
        return filename + '.gz'
    return None

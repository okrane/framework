from cheuvreux.dbtools.Sqlite import SQLiteBase
from cheuvreux.dbtools.odbc import ODBC
import locale
import os

dirname = os.path.dirname

DB = ODBC('DRIVER={SQL Server};SERVER=nysql001;DATABASE=quant;UID=syarc;PWD=syarc')
#DB_old = SQLiteBase(os.path.join(dirname(dirname(__file__)), 'db\orders.db'))

locale.setlocale(locale.LC_ALL, '')


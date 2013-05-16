'''
Created on April 25, 2012

@author: jcmau

- check for Fidessa instant chat PSV
'''
import os
from datetime import datetime
from cheuvreux import fidessa
from cheuvreux.stdio.mail import HtmlEmail, table_style

date = datetime.today().date()
file = fidessa.find_file('ORDER_EVENTS.%s.psv', date.strftime('%Y%m%d'))

if file and os.path.exists(file):
    email = HtmlEmail('smtpnotes', style=table_style)
    email.set_sender('jimperato')
    email.set_dest('newyork.compliance@cheuvreux.com, sarchenault, maquino, dzack, jimperato')
    email.set_subject('Fidessa chat extract ' + date.isoformat())
    email.attachFile(file)
    email.flush()
else:
    email = HtmlEmail('smtpnotes', style=table_style)
    email.set_sender('jimperato')
    email.set_dest('newyork.compliance@cheuvreux.com, sarchenault, maquino, dzack, jimperato')
    email.set_subject('No Fidessa chat extract created ' + date.isoformat())

    email.flush()



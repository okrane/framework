import smtplib
from lib.logger import *
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def date_to_datetime(dates):
    dates_datetime = []
    for d in dates:
        dates_datetime.append(datetime.strptime(d, '%Y%m%d'))
    dates_datetime = sorted(dates_datetime)
    return dates_datetime 

def get_traceback():
    import traceback
    import StringIO
    fp = StringIO.StringIO()
    traceback.print_exc(file = fp)
    logging.error(fp.getvalue())

def send_email(_to      = ["quant.algo@keplerchevreux.com"], 
               _from    = "quant.algo@keplerchevreux.com",
               _subject = "My subject",
               _message = ""):

    # Create a text/plain message
    msg             = MIMEMultipart("alternative")
    msg['Subject']  = '%s' % _subject
    msg['From']     = _from
    msg['To']       = ";".join(_to)
    html            = MIMEText(_message, 'html')
    msg.attach(html)
    
    try:
        smtpObj = smtplib.SMTP('172.29.97.16')
        smtpObj.sendmail( _from,  _to, msg.as_string())         
        logging.info("Successfully sent email")
    except smtplib.SMTPException,e :
        logging.error("Error: unable to send email")
        get_traceback()
    finally:
        try:
            smtpObj.quit()
        except:
            import time
            import random
            s = '__' + str(random.randint(1, 1000))
            file = open(str(time.time()) + s + ".html", "w")
            file.write(_message)
            file.close()
 
       
if __name__ == "__main__":
    send_email(_to = ["alababidi@keplercheuvreux.com"],
               _message= "<h1>Salut</h1>")
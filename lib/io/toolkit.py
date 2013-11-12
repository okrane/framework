import smtplib
from lib.logger import *
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os
import paramiko
import sys
import xml.etree.ElementTree as ET
logging.getLogger("paramiko").setLevel(logging.WARNING)

def last_business_day(date=None):
    """date is a datetime"""
    if date is None:
        date = datetime.now()
    if date.weekday() in [0, 6]: # Monday, Sunday
        last = date - timedelta(days = 3 - ( (7 - date.weekday() ) %7) )
    else:
        last = date - timedelta(days = 1)
    return last

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
               _message = "",
               _files = None):
    
    # Create a text/plain message
    msg             = MIMEMultipart("alternative")
    msg['Subject']  = '%s' % _subject
    msg['From']     = _from
    msg['To']       = ";".join(_to)
    msg.attach( MIMEText(_message, 'html') )
    
    if _files is not None:
        if isinstance(_files,basestring):
            _files = [_files]
            
        for f in _files:
            outf = open(f,"rb")
            part = MIMEBase('application', "octet-stream")
            part.set_payload(outf.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
            msg.attach(part)
            outf.close()

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

#-------------------
#- FILES HANDLING ON DIFFERENt SERVER
#-------------------

def send(local, remote , server_remote = 'PARFLTLAB' , env = 'dev' ,  user = 'flexapp'):
    conf                 = get_conf(env)
    server               = conf[server_remote]
    ip                   = server['ip_addr']
    username             = server['list_users'][user]['username']
    password             = server['list_users'][user]['passwd']
       
    transport = paramiko.Transport((ip, 22))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.put(local, remote)
    fexist = check(sftp,remote)
    sftp.close()
    
    return fexist

def check(sftp, path):
    try:
        s = sftp.stat(path)
        logging.info('Successful sent at ' + path)
    except IOError as e:
        logging.error(e)
        get_traceback()
        return False
        
    return True

def get_conf(referential):
    # -- get universe dico file
    full_path            = os.path.realpath(__file__)    
    path, f              = os.path.split(full_path)        
    universe_file        = os.path.join(path, 'KC_universe.xml')
    
    # -- do
    conf        = {}
    struct      = ET.parse(universe_file)
    raw_data    = struct.getroot()
    servs_dico  = {}
    
    for elt in raw_data.findall('env'):
        if elt.get('name') == referential:
            servs_dico = elt
            
    if servs_dico != {}:
        for attr in servs_dico.findall('server'):
            serv_name   = attr.get('name')
            dict_server = {'ip_addr': attr.get('ip_addr')}
            l_users     = {}
            
            for u_attr in attr.findall('user'):
                l_users[u_attr.get('username')] = u_attr.attrib
                
            dict_server['list_users']   = l_users
            conf[serv_name]             = dict_server
            
    return conf


      
if __name__ == "__main__":
    send_email(_to = ["alababidi@keplercheuvreux.com"], _message= "<h1>Salut</h1>")
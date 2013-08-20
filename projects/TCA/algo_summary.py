from lib.tca.wrapper import DataProcessor
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from lib.io.toolkit import send_email
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
import smtplib
import os

if __name__=='__main__':
    from lib.dbtools.connections import Connections
#     Connections.change_connections("dev")
    
    
    day = datetime(year=2013, month=7, day=23)
    day = datetime.now() - timedelta(days=1)
    if os.name == 'nt':
        folder  = 'C:\\temp\\'
    else:
        folder  = '~/temp/'
    l       = []
    
    
    # Daily
    daily = DataProcessor(start_date = day, end_date = day)
    daily.plot_algo_volume()
    filename = folder + 'Vol_euro_from_' + datetime.strftime(day, '%Y%m%d' ) + 'to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    plt.savefig( filename )
    l.append(filename)
    
    daily.plot_algo_occ()
    filename = folder + 'Occ_euro_from_' + datetime.strftime(day, '%Y%m%d' ) + 'to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    plt.savefig( filename )
    l.append(filename)
    
    # Weekly
    weekly = DataProcessor(start_date = day - timedelta(days=7), end_date = day)
    weekly.plot_algo_volume()
    filename = folder + 'Vol_euro_from_' + datetime.strftime(day- timedelta(days=7), '%Y%m%d' ) + 'to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    plt.savefig( filename )
    l.append(filename)
    
    weekly.plot_algo_occ()
    filename = folder + 'Occ_euro_from_' + datetime.strftime(day- timedelta(days=7), '%Y%m%d' ) + 'to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    plt.savefig( filename )
    l.append(filename)
    
     
    # Monthly
    monthly = DataProcessor(start_date = day - timedelta(days=28), end_date = day)
    monthly.plot_algo_volume()
    filename = folder + 'Vol_euro_from_' + datetime.strftime(day - timedelta(days=28), '%Y%m%d' ) + 'to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    plt.savefig( filename )
    l.append(filename)
    
    
    monthly.plot_algo_occ()
    filename = folder + 'Occ_euro_from_' + datetime.strftime(day - timedelta(days=28), '%Y%m%d' ) + 'to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    plt.savefig( filename )
    l.append(filename)
    
    # Send an email
    
    title = "<h2>Sum up for Yesterday %s</h2>\n" % datetime.strftime(day, '%Y%m%d' )
        
    # Create the container (outer) email message.
    msg = MIMEMultipart()
    msg['Subject'] = 'test'
    # me == the sender's email address
    # family = the list of all recipients' email addresses
    msg['From'] = 'alababidi@keplercheuvreux.com'
    msg['To'] = 'alababidi@keplercheuvreux.com'
    
    # Assume we know that the image files are all in PNG format
    for file in l:
        # Open the files in binary mode.  Let the MIMEImage class automatically
        # guess the specific image type.
        fp = open(file, 'rb')
        img = MIMEImage(fp.read())
        fp.close()
        msg.attach(img)
    # Send the email via our own SMTP server.
    s = smtplib.SMTP('172.29.97.16')
    s.sendmail(msg['To'], msg['From'], msg.as_string())
    s.quit()
import matplotlib

import os
from email.mime.text import MIMEText
if os.name != 'nt':
    ### VERY IMPROTANT IN LINUX (in order to use X11)
    matplotlib.use('Agg')
from lib.tca.wrapper import DataProcessor
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from lib.io.toolkit import send_email
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
import smtplib


if __name__=='__main__':
    from lib.dbtools.connections import Connections
#     Connections.change_connections("dev")
    
    
    day = datetime(year=2013, month=7, day=23)
    day = datetime.now() - timedelta(days=1)
    if os.name == 'nt':
        folder  = 'C:\\temp\\'
    else:
        folder  = '/home/quant/temp/'
    l       = []
    
    msg = MIMEMultipart('alternative')
    msg['Subject'] = 'Algo Summary'
    
    
    
    # Daily
    m = '<h2>Daily<h2>'
    daily = DataProcessor(start_date = day, end_date = day)
    
    
    daily.plot_algo_volume()
    image_name = 'Vol_euro_from_' + datetime.strftime(day, '%Y%m%d' ) + 'to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    file_path = folder + image_name
    plt.savefig( file_path )
    l.append(image_name)
    
    
    daily.plot_algo_occ()
    image_name = 'Occ_euro_from_' + datetime.strftime(day, '%Y%m%d' ) + 'to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    file_path = folder + image_name
    plt.savefig( file_path )
    l.append(image_name)
    
    
    
    
    # Weekly
    m += '<h2>Weekly<h2>'
    weekly = DataProcessor(start_date = day - timedelta(days=7), end_date = day)
    
    
    weekly.plot_algo_volume()
    image_name = 'Vol_euro_from_' + datetime.strftime(day- timedelta(days=7), '%Y%m%d' ) + 'to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    file_path = folder + image_name
    plt.savefig( file_path )
    l.append(image_name)
    
    weekly.plot_algo_occ()
    image_name = 'Occ_euro_from_' + datetime.strftime(day- timedelta(days=7), '%Y%m%d' ) + 'to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    file_path = folder + image_name
    plt.savefig( file_path )
    l.append(image_name)
    
    
    
    # Monthly
    m += '<h2>Monthly<h2>' 
    monthly = DataProcessor(start_date = day - timedelta(days=28), end_date = day)
    
    
    monthly.plot_algo_volume()
    image_name = 'Vol_euro_from_' + datetime.strftime(day - timedelta(days=28), '%Y%m%d' ) + 'to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    file_path = folder + image_name
    plt.savefig( file_path )
    l.append(image_name)
    
    
    monthly.plot_algo_occ()
    image_name = 'Occ_euro_from_' + datetime.strftime(day - timedelta(days=28), '%Y%m%d' ) + 'to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    file_path = folder + image_name
    plt.savefig( file_path )
    l.append(image_name)
    
    # Send an email
    
    title = "<h2>Sum up for %s</h2>\n" % datetime.strftime(day, '%Y%m%d' )
        
    # Create the container (outer) email message.
    
    
    # me == the sender's email address
    # family = the list of all recipients' email addresses
    msg['From'] = 'alababidi@keplercheuvreux.com'
    to = ['njoseph@keplercheuvreux.com', 'alababidi@keplercheuvreux.com']
    msg['To'] = ' ,'.join(to)
    # Assume we know that the image files are all in PNG format
    for file in l:
        # Open the files in binary mode.  Let the MIMEImage class automatically
        # guess the specific image type.
        m += '<b>Some <i>HTML</i> text</b> and an image.<br><img src="cid:%s"><br>Nifty!'%file
        fp = open(folder + file, 'rb')
        img = MIMEImage(fp.read())
        img.add_header('Content-ID', '<%s>'%file)
        fp.close()
        msg.attach(img)
    # Send the email via our own SMTP server.
    s = smtplib.SMTP('172.29.97.16')
    s.sendmail(msg['From'], to, msg.as_string())
    s.quit()
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
    
    
    msg = MIMEMultipart()
    msg['Subject'] = 'Algo Summary (Beta test, weekly/monthly are still wrong)'
    
    
    
    # Daily
    m = '<h3>Daily</h3>'
    daily = DataProcessor(start_date = day, end_date = day)
    
    l         = []
    list_path = []
    
    def  repeat(name):
        global l
        global list_path
        
        file_path = folder + image_name
        
        l.append(image_name)
        list_path.append(file_path)
        
    image_name = 'Vol_euro_from_' + datetime.strftime(day, '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    repeat(image_name)
    
    image_name = 'Occ_euro_from_' + datetime.strftime(day, '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    repeat(image_name)
    
    image_name = 'Place_from_' + datetime.strftime(day, '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    repeat(image_name)
    
    image_name = 'Intraday_algo_vol_from_' + datetime.strftime(day, '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    file_path = folder + image_name
    repeat(image_name)
    
    daily.plot_basic_stats(path=list_path[0:3])
    daily.plot_intraday_exec_curve().savefig(file_path)
    
    for image in l:
        m += '<img src="cid:%s">\n' %image
        
    # Weekly
    m += '<h3>Weekly</h3>'
    weekly = DataProcessor(start_date = day - timedelta(days=7), end_date = day)
    
    image_name = 'Vol_euro_from_' + datetime.strftime(day- timedelta(days=7), '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    repeat(image_name)
    
    image_name = 'Occ_euro_from_' + datetime.strftime(day- timedelta(days=7), '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    repeat(image_name)
    
    image_name = 'Place_from_' + datetime.strftime(day- timedelta(days=7), '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    repeat(image_name)
    
    weekly.plot_basic_stats(path=list_path[4:7])
    
    for image in l:
        m += '<img src="cid:%s">\n' %image
        
                
    # Monthly
    m += '<h3>Monthly</h3>' 
    monthly = DataProcessor(start_date = day - timedelta(days=28), end_date = day)
    
    image_name = 'Vol_euro_from_' + datetime.strftime(day - timedelta(days=28), '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    repeat(image_name)

    image_name = 'Occ_euro_from_' + datetime.strftime(day - timedelta(days=28), '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    repeat(image_name)
    
    image_name = 'Place_from_' + datetime.strftime(day - timedelta(days=28), '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    repeat(image_name)
 
    monthly.plot_basic_stats(path=list_path[4:7])
    
    for image in l:
        m += '<img src="cid:%s">\n' %image

    # Send an email
    title = "<h2>Sum up for %s</h2>\n" % datetime.strftime(day, '%Y%m%d' )
        

    
    # me == the sender's email address
    # family = the list of all recipients' email addresses
    msg['From'] = 'alababidi@keplercheuvreux.com'
    to = ['alababidi@keplercheuvreux.com']
    msg['To'] = ' ,'.join(to)
    # Assume we know that the image files are all in PNG format
    for file in l:
        # Open the files in binary mode.  Let the MIMEImage class automatically
        # guess the specific image type.
        fp = open(folder + file, 'rb')
        img = MIMEImage(fp.read())
        img.add_header('Content-ID', '<%s>'%file)
        fp.close()
        msg.attach(img)
    msg.attach(MIMEText(m, 'html'))     
    # Send the email via our own SMTP server.
    s = smtplib.SMTP('172.29.97.16')
    s.sendmail(msg['From'], to, msg.as_string())
    s.quit()
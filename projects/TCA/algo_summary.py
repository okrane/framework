import matplotlib

import os
from email.mime.text import MIMEText
if os.name != 'nt':
    ### VERY IMPROTANT IN LINUX (in order to use X11)
    matplotlib.use('Agg')
from lib.tca.wrapper import PlotEngine
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from lib.io.toolkit import send_email
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
import smtplib
from lib.logger.custom_logger import *
import logging

import lib.tca.algoplot
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
    msg['Subject'] = 'Algo Summary (Beta test)'
    

    daily = PlotEngine(start_date = day, end_date = day)
    
    l         = []
    list_path = []
    
    def  repeat(name):
        global l
        global list_path
        
        file_path = folder + image_name
        
        l.append(image_name)
        list_path.append(file_path)
    

        
    sdate = day - timedelta(days=90)
    edate = day 
    from lib.tca.algostats import AlgoStatsProcessor
    algo_data = AlgoStatsProcessor(start_date = sdate, end_date = edate)
    algo_data.get_db_data(level='sequence',force_colnames_only=['strategy_name_mapped','rate_to_euro','turnover','TargetSubID','ExDestination'])
    
    
    #Sum up
    history = lib.tca.algoplot.PlotEngine()
    h, data = history.plot_algo_evolution(algo_data=algo_data,level='sequence',var='mturnover_euro',gvar='is_dma')
    image_name = 'Serie_daily_' + datetime.strftime(sdate, '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    repeat(image_name)
    h.savefig(folder + image_name)
    m = '<h2>History</h2>'
    m += '<img src="cid:%s">\n' %(folder + image_name)
    
    # Daily
    m += '<h2>Daily</h2>'
    image_name = 'Vol_euro_from_' + datetime.strftime(day, '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    repeat(image_name)
    
    image_name = 'Occ_euro_from_' + datetime.strftime(day, '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    repeat(image_name)
    
    image_name = 'Place_from_' + datetime.strftime(day, '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    repeat(image_name)
    
    image_name = 'Intraday_algo_vol_from_' + datetime.strftime(day, '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    file_path = folder + image_name
    repeat(image_name)
    
    daily.plot_basic_stats(path=list_path[1:4])
    daily.plot_intraday_exec_curve().savefig(file_path)
    
    for image in l[1:5]:
        m += '<img src="cid:%s">\n' %image
        
    # Weekly
    m += '<h2>Weekly</h2>'
    weekly = PlotEngine(start_date = day - timedelta(days=7), end_date = day)
    
    image_name = 'Vol_euro_from_' + datetime.strftime(day- timedelta(days=7), '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    repeat(image_name)
    
    image_name = 'Occ_euro_from_' + datetime.strftime(day- timedelta(days=7), '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    repeat(image_name)
    
    image_name = 'Place_from_' + datetime.strftime(day- timedelta(days=7), '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    repeat(image_name)
    
    image_name = 'Intraday_algo_vol_from_' + datetime.strftime(day- timedelta(days=7), '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    file_path = folder + image_name
    repeat(image_name)
    
    weekly.plot_basic_stats(path=list_path[5:8])
    weekly.plot_intraday_exec_curve().savefig(file_path)
    
    for image in l[5:9]:
        m += '<img src="cid:%s">\n' %image
        
                
    # Monthly
    m += '<h2>Monthly</h2>' 
    monthly = PlotEngine(start_date = day - timedelta(days=28), end_date = day)
    
    image_name = 'Vol_euro_from_' + datetime.strftime(day - timedelta(days=28), '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    repeat(image_name)

    image_name = 'Occ_euro_from_' + datetime.strftime(day - timedelta(days=28), '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    repeat(image_name)
    
    image_name = 'Place_from_' + datetime.strftime(day - timedelta(days=28), '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    repeat(image_name)
 
    image_name = 'Intraday_algo_vol_from_' + datetime.strftime(day- timedelta(days=28), '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    file_path = folder + image_name
    repeat(image_name)
    
    monthly.plot_basic_stats(path=list_path[9:12])
    monthly.plot_intraday_exec_curve().savefig(file_path)
    for image in l[9:13]:
        m += '<img src="cid:%s">\n' %image

    # Send an email
    title = "<h2>Sum up for %s</h2>\n" % datetime.strftime(day, '%Y%m%d' )
        
    
    
    # me == the sender's email address
    # family = the list of all recipients' email addresses
    msg['From'] = 'alababidi@keplercheuvreux.com'
    to = ['algoquant@keplercheuvreux.com', 'mnamajee@keplercheuvreux.com', 'glin@keplercheuvreux.com']
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
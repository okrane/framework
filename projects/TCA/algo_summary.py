import matplotlib

import os
from email.mime.text import MIMEText
if os.name != 'nt':
    ### VERY IMPROTANT IN LINUX (in order to use X11)
    matplotlib.use('Agg')

from lib.tca.wrapper import PlotEngine
from datetime import datetime, timedelta, time
import pytz
import matplotlib.pyplot as plt
from lib.io.toolkit import send_email
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
import smtplib
from lib.logger.custom_logger import *
import logging
import lib.tca.algoplot
import numpy as np
import lib.data.dataframe_tools as dftools
import lib.stats.slicer as slicer
from lib.tca.algostats import AlgoStatsProcessor


def plot_evol_perf(data,algo):
    tmp = data[data['occ_fe_strategy_name_mapped'] == algo.upper()]
    if tmp.shape[0] == 0:
        return None
    
    out = plt.figure()
    plt.hold(True)
    plt.plot([x.to_datetime() for x in tmp['tmp_date_start']],tmp['Slippage Vwap (mean / bp)'],'o-')
    plt.plot([x.to_datetime() for x in tmp['tmp_date_start']],tmp['IC down : Vwap(mean / bp)'],'-',color = 'r')
    plt.plot([x.to_datetime() for x in tmp['tmp_date_start']],tmp['IC up : Vwap(mean / bp)'],'-',color = 'g')
    plt.hold(False)
    plt.title('Slippage evolution: ' + algo)
    # plt.legend('Mean','IC down','IC up')
    
    return out


if __name__=='__main__':
    from lib.dbtools.connections import Connections
#     Connections.change_connections("dev")
    
    now = datetime.now() - timedelta(days=3)
    day = datetime(year = now.year, month=now.month, day=now.day, hour = 23, minute = 59, second = 59)
    
    
    #day = datetime.combine(.day, date).time() - timedelta(days=1)
    if os.name == 'nt':
        folder  = 'C:\\temp\\'
    else:
        folder  = '/home/quant/temp/'
    
    
    msg = MIMEMultipart()
    msg['Subject'] = 'Algo Summary (Beta test)'
    
    # me == the sender's email address
    # family = the list of all recipients' email addresses
    msg['From'] = 'alababidi@keplercheuvreux.com'
    #to = ['algoquant@keplercheuvreux.com', 'mnamajee@keplercheuvreux.com', 'glin@keplercheuvreux.com']
    to = ['alababidi@keplercheuvreux.com','njoseph@keplercheuvreux.com']
    msg['To'] = ' ,'.join(to)

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
    algo_data = AlgoStatsProcessor(start_date = sdate, end_date = edate)
    algo_data.get_db_data(level='sequence',force_colnames_only=['strategy_name_mapped','rate_to_euro','turnover','TargetSubID','ExDestination'])
    
    
    #Sum up
    history = lib.tca.algoplot.PlotEngine()
    h, data = history.plot_algo_evolution(algo_data=algo_data,level='sequence',var='mturnover_euro',gvar='is_dma')
    image_name = 'Serie_daily_' + datetime.strftime(sdate, '%Y%m%d' ) + '_to_' + datetime.strftime(day, '%Y%m%d' ) + '.png'
    repeat(image_name)
    h.savefig(folder + image_name)
    m = '<h2>History</h2>'
    m += '<img src="cid:%s">\n' %image_name
    #plt.show()
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
    
    ###########################################################################
    # Slippage table.
    ###########################################################################
    startday = datetime(year = 2013, month=1, day=1, hour = 0, minute = 0, second = 1)
    sday = datetime(year = now.year, month=now.month, day=now.day, hour = 0, minute = 0, second = 1)
    mday = datetime.now() - timedelta(days=28)
    mday = datetime(year = mday.year, month=mday.month, day=mday.day, hour = 0, minute = 0, second = 1)
    
    #--- extract data
    occ_data_4slippage = AlgoStatsProcessor(start_date = startday, end_date = day)
    occ_data_4slippage.get_occ_fe_data()
    occ_data_4slippage = occ_data_4slippage.data_occurrence.copy()
    occ_data_dates = [x.to_datetime() for x in occ_data_4slippage.index]
    
    #--- stats defintion
    STATS = {'nb': lambda df : len(df.p_occ_id),
         'nb_slippage_vwap_bp': lambda df : len(np.where((np.isfinite(df.rate_to_euro)) & (np.isfinite(df.slippage_vwap_bp)) & (df.occ_fe_turnover*df.rate_to_euro>0))[0]),
         'Slippage Vwap (mean / bp)': lambda df : slicer.weighted_statistics(df.slippage_vwap_bp.values,df.occ_fe_turnover.values*df.rate_to_euro.values,mode='mean'),
         'Slippage Vwap (std / bp)': lambda df : slicer.weighted_statistics(df.slippage_vwap_bp.values,df.occ_fe_turnover.values*df.rate_to_euro.values,mode='std'),
         'nb_slippage_is_bp': lambda df : len(np.where((np.isfinite(df.rate_to_euro)) & (np.isfinite(df.slippage_is_bp)) & (df.occ_fe_turnover*df.rate_to_euro>0))[0]),
         'Slippage IS (mean / bp)': lambda df : slicer.weighted_statistics(df.slippage_is_bp.values,df.occ_fe_turnover.values*df.rate_to_euro.values,mode='mean'),
         'Slippage IS (std / bp)': lambda df : slicer.weighted_statistics(df.slippage_is_bp.values,df.occ_fe_turnover.values*df.rate_to_euro.values,mode='std'),
         'Spread (mean / bp)' : lambda df : slicer.weighted_statistics(df.avg_spread_bp.values,df.occ_fe_turnover.values*df.rate_to_euro.values,mode='mean')}
                             
    #--- add daily table
    m += '<h2>Slippage Daily (from FlexStat)</h2>'
    tmp_ = occ_data_4slippage[ map(lambda x : x >= pytz.UTC.localize(sday) and x <= pytz.UTC.localize(day), occ_data_dates) ]
    if tmp_.shape[0] == 0:
        m += 'No Data'
    else:
        agg_data = dftools.agg(tmp_,
                       group_vars = ['occ_fe_strategy_name_mapped'],
                       stats = STATS)
        
        m += agg_data[['occ_fe_strategy_name_mapped','Slippage Vwap (mean / bp)','Slippage Vwap (std / bp)',
              'Slippage IS (mean / bp)','Slippage IS (std / bp)','Spread (mean / bp)']].to_html()
              
    #--- add monthly table
    m += '<h2>Slippage Monthly (from FlexStat)</h2>'
    tmp_ = occ_data_4slippage[ map(lambda x : x >= pytz.UTC.localize(mday) and x <= pytz.UTC.localize(day), occ_data_dates) ]
    if tmp_.shape[0] == 0:
        m += 'No Data'
    else:
        agg_data = dftools.agg(tmp_,
                       group_vars = ['occ_fe_strategy_name_mapped'],
                       stats = STATS)
        
        m += agg_data[['occ_fe_strategy_name_mapped','Slippage Vwap (mean / bp)','Slippage Vwap (std / bp)',
              'Slippage IS (mean / bp)','Slippage IS (std / bp)','Spread (mean / bp)']].to_html()
          
    #--- evolution weekly des perfs 
    occ_data_4slippage['tmp_date_end'] = [datetime.combine(x.to_datetime().date()-timedelta(days=x.to_datetime().date().weekday()-4),time(0,0,0)) for x in occ_data_4slippage.index]
    occ_data_4slippage['tmp_date_start'] = [datetime.combine(x.to_datetime().date()-timedelta(days=x.to_datetime().date().weekday()),time(0,0,0)) for x in occ_data_4slippage.index]
    
    agg_weekly_data = dftools.agg(occ_data_4slippage,
                                 group_vars = ['tmp_date_start','tmp_date_end','occ_fe_strategy_name_mapped'],
                                 stats = STATS)
                                 
    agg_weekly_data['IC down : Vwap(mean / bp)'] = (agg_weekly_data['Slippage Vwap (mean / bp)']
                                        - 1.96 *  agg_weekly_data['Slippage Vwap (std / bp)'] / np.sqrt(agg_weekly_data['nb_slippage_vwap_bp']))
                                        
    agg_weekly_data['IC up : Vwap(mean / bp)'] = (agg_weekly_data['Slippage Vwap (mean / bp)']
                                        + 1.96 *  agg_weekly_data['Slippage Vwap (std / bp)'] / np.sqrt(agg_weekly_data['nb_slippage_vwap_bp']))
                                        
    agg_weekly_data['IC down : IS(mean / bp)'] = (agg_weekly_data['Slippage IS (mean / bp)']
                                        - 1.96 *  agg_weekly_data['Slippage IS (std / bp)'] / np.sqrt(agg_weekly_data['nb_slippage_is_bp']))
                                        
    agg_weekly_data['IC up : IS(mean / bp)'] = (agg_weekly_data['Slippage IS (mean / bp)']
                                        + 1.96 *  agg_weekly_data['Slippage IS (std / bp)'] / np.sqrt(agg_weekly_data['nb_slippage_is_bp']))
                                        
    algo_evol = [ 'Vwap' , 'VOL' ,'DYNVOL']
    # attention a gerer dans le plot si le bench n'est pas le vwap+ un dico avant
    for algo in algo_evol:
        h = plot_evol_perf(agg_weekly_data,algo)
        if h is not None:
            m += '<h2>' + algo + ': weekly Slippage Evolution (from FlexStat)</h2>'
            image_name = 'evol_' + algo
            h.savefig(folder + image_name)
            repeat(image_name)
            m += '<img src="cid:%s">\n' %image_name
    
    
    ###########################################################################
    # add figure to email msg.
    ###########################################################################   
    
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
    
    
    ###########################################################################
    # Send the email via our own SMTP server.
    ###########################################################################
    s = smtplib.SMTP('172.29.97.16')
    s.sendmail(msg['From'], to, msg.as_string())
    s.quit()
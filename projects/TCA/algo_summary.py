import matplotlib

import os
from email.mime.text import MIMEText
if os.name != 'nt':
    ### VERY IMPROTANT IN LINUX (in order to use X11)
    matplotlib.use('Agg')

from lib.tca.wrapper import PlotEngine, DEFAULT_FIGSIZE
from datetime import datetime, timedelta, time
import pytz
import matplotlib.pyplot as plt
from lib.io.toolkit import send_email
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email import Encoders
import smtplib
from lib.logger.custom_logger import *
import logging
import lib.tca.algoplot
import numpy as np
import lib.data.dataframe_tools as dftools
import lib.stats.slicer as slicer
from lib.tca.algostats import AlgoStatsProcessor
from latex_template import latex_string
from subprocess import call

if os.name == 'nt':
    FOLDER  = 'C:\\temp\\'
    LATEX   = '"C:\\Program Files\\MiKTeX 2.9\\miktex\\bin\\pdflatex"'
else:
    FOLDER  = '/home/quant/temp/'
    LATEX   = '/home/quant/tex/bin/x86_64-linux/pdflatex'

def round_spe(x):
    if not isinstance(x, basestring): 
        return round(x,2)
    return x

def plot_evol_perf(data,algo):
    tmp = data[data['occ_fe_strategy_name_mapped'] == algo.upper()]
    if tmp.shape[0] == 0:
        return None
    
    out = plt.figure(figsize = DEFAULT_FIGSIZE)
    dates = [x.to_datetime() for x in tmp['tmp_date_start']]
    plt.hold(True)
    plt.plot(dates,tmp['Slippage Vwap (mean / bp)'],'o-')
    plt.plot(dates,tmp['IC down : Vwap(mean / bp)'],'-',color = 'r')
    plt.plot(dates,tmp['IC up : Vwap(mean / bp)'],'-',color = 'g')
    plt.plot([dates[0],dates[-1]],[0,0],'--',color = 'k')
    plt.hold(False)
    plt.title('Slippage evolution: ' + algo)
    plt.legend(['Mean','IC down','IC up'] , loc='upper left')
    plt.ylabel('Slippage Vwap (bp)')
    
    return out


def plot_basic_stats(start, end, path_list, image_name_list, html_string):
    new_path_list = []
    new_image_name_list = []
    duration = datetime.strftime(start, '%Y%m%d' ) + '_to_' + datetime.strftime(end, '%Y%m%d' ) + '.png'
    image_name = 'Vol_euro_from_' + duration
    new_path_list.append(FOLDER + image_name)
    new_image_name_list.append(image_name)
    
    image_name = 'Occ_euro_from_' + duration
    new_path_list.append(FOLDER + image_name)
    new_image_name_list.append(image_name)
    
    image_name = 'Place_from_' + duration
    new_path_list.append(FOLDER + image_name)
    new_image_name_list.append(image_name)
    
    image_name = 'Intraday_algo_vol_from_' + duration
    new_path_list.append(FOLDER + image_name)
    new_image_name_list.append(image_name)
        
    period = PlotEngine(start_date  = start, end_date = end)
    period.plot_basic_stats(path    = new_path_list)
    period.plot_intraday_exec_curve(duration = 'From ' + datetime.strftime(start, '%Y/%m/%d' ) + ' to ' + datetime.strftime(end, '%Y/%m/%d' )).savefig(FOLDER + image_name)
    
    for image in new_image_name_list:
        html_string += '<img src="cid:%s">\n' %image
    
    path_list.extend(new_path_list)
    image_name_list.extend(new_image_name_list)
    
    return html_string


if __name__=='__main__':
    from lib.dbtools.connections import Connections
    #     Connections.change_connections("dev")
    
    now     = datetime.now() - timedelta(days=1)
    day     = datetime(year = now.year, month=now.month, day=now.day, hour = 23, minute = 59, second = 59)
    day_str = datetime.strftime(day, '%Y%m%d' )
    #doc = SimpleDocTemplate(FOLDER + "Sum_up_" + datetime.strftime(day, '%Y/%m/%d') + ".pdf", pagesize=letter)
    msg = MIMEMultipart()
    msg['Subject'] = 'Algo Summary (Beta test)'
    msg['From'] = 'alababidi@keplercheuvreux.com'
    to = ['algoquant@keplercheuvreux.com', 'mnamajee@keplercheuvreux.com', 'glin@keplercheuvreux.com']
    #to = ['alababidi@keplercheuvreux.com','njoseph@keplercheuvreux.com']
    msg['To'] = ' ,'.join(to)
    
    list_image  = []
    list_path   = []
    list_tables = []
    html_string = ""    
    
    ###########################################################################
    # History
    ###########################################################################   
    sdate = day - timedelta(days=90)
    edate = day
    algo_data = AlgoStatsProcessor(start_date = sdate, end_date = edate)
    algo_data.get_db_data(level='sequence',force_colnames_only=['strategy_name_mapped','rate_to_euro','turnover','TargetSubID','ExDestination'])
    
    history = lib.tca.algoplot.PlotEngine()
    h, data = history.plot_algo_evolution(algo_data=algo_data,level='sequence',var='mturnover_euro',gvar='is_dma')
    image_name = 'Serie_daily_' + datetime.strftime(sdate, '%Y%m%d' ) + '_to_' + day_str + '.png'
    
    list_image.append(image_name)
    list_path.append(FOLDER + image_name)
    h.savefig(FOLDER + image_name)
    
    html_string += '<h2>History</h2>'
    html_string += '<img src="cid:%s">\n' %image_name
    #plt.show()

    ###########################################################################
    # Algo Volume
    ###########################################################################  
    # Daily
    html_string += '<h2>Daily</h2>'
    html_string = plot_basic_stats(edate, edate, list_path, list_image, html_string)
    #plt.show()
       
    # Weekly
    html_string += '<h2>Weekly</h2>'
    html_string = plot_basic_stats(day - timedelta(days=7), edate, list_path, list_image, html_string)
    #plt.show() 
               
    # Monthly
    html_string += '<h2>Monthly</h2>'
    html_string = plot_basic_stats(day - timedelta(days=28), edate, list_path, list_image, html_string)
    #plt.show()
    
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
         'nb_slippage_vwap_bp': lambda df : len(np.where((np.isfinite(df.rate_to_euro)) & (np.isfinite(df.slippage_vwap_bp)) & (df.occ_fe_avg_price.apply(lambda x : float(x))*df.occ_fe_exec_shares*df.rate_to_euro>0))[0]),
         'Slippage Vwap (mean / bp)': lambda df : slicer.weighted_statistics(df.slippage_vwap_bp.values,df.occ_fe_avg_price.apply(lambda x : float(x))*df.occ_fe_exec_shares*df.rate_to_euro,mode='mean'),
         'Slippage Vwap (std / bp)': lambda df : slicer.weighted_statistics(df.slippage_vwap_bp.values,df.occ_fe_avg_price.apply(lambda x : float(x))*df.occ_fe_exec_shares*df.rate_to_euro,mode='std'),
         'nb_slippage_is_bp': lambda df : len(np.where((np.isfinite(df.rate_to_euro)) & (np.isfinite(df.slippage_is_bp)) & (df.occ_fe_avg_price.apply(lambda x : float(x))*df.occ_fe_exec_shares*df.rate_to_euro>0))[0]),
         'Slippage IS (mean / bp)': lambda df : slicer.weighted_statistics(df.slippage_is_bp.values,df.occ_fe_avg_price.apply(lambda x : float(x))*df.occ_fe_exec_shares*df.rate_to_euro,mode='mean'),
         'Slippage IS (std / bp)': lambda df : slicer.weighted_statistics(df.slippage_is_bp.values,df.occ_fe_avg_price.apply(lambda x : float(x))*df.occ_fe_exec_shares*df.rate_to_euro,mode='std'),
         'Spread (mean / bp)' : lambda df : slicer.weighted_statistics(df.avg_spread_bp.values,df.occ_fe_avg_price.apply(lambda x : float(x))*df.occ_fe_exec_shares*df.rate_to_euro,mode='mean')}
                             
    #--- add daily table
    html_string += '<h2>Slippage Daily (from FlexStat)</h2>'
    tmp_ = occ_data_4slippage[ map(lambda x : x >= pytz.UTC.localize(sday) and x <= pytz.UTC.localize(day), occ_data_dates) ]
    if tmp_.shape[0] == 0:
        html_string += 'No Data'
        list_tables.append('No Data')
    else:
        agg_data = dftools.agg(tmp_,
                       group_vars = ['occ_fe_strategy_name_mapped'],
                       stats = STATS)
        
        agg_data = agg_data.rename(columns={'occ_fe_strategy_name_mapped': 'Strategy'})
        slippage_df = agg_data[['Strategy','Slippage Vwap (mean / bp)','Slippage Vwap (std / bp)',
              'Slippage IS (mean / bp)','Slippage IS (std / bp)','Spread (mean / bp)']]
        slippage_df = slippage_df.applymap(round_spe)
        html_string += slippage_df.to_html()
        list_tables.append(slippage_df.to_latex())       
    #--- add monthly table
    html_string += '<h2>Slippage Monthly (from FlexStat)</h2>'
    tmp_ = occ_data_4slippage[ map(lambda x : x >= pytz.UTC.localize(mday) and x <= pytz.UTC.localize(day), occ_data_dates) ]
    if tmp_.shape[0] == 0:
        html_string += 'No Data'
        list_tables.append('No Data')
    else:
        agg_data = dftools.agg(tmp_,
                       group_vars = ['occ_fe_strategy_name_mapped'],
                       stats = STATS)
        
        agg_data = agg_data.rename(columns={'occ_fe_strategy_name_mapped': 'Strategy'})
        slippage_df = agg_data[['Strategy','Slippage Vwap (mean / bp)','Slippage Vwap (std / bp)',
              'Slippage IS (mean / bp)','Slippage IS (std / bp)','Spread (mean / bp)']]
        slippage_df = slippage_df.applymap(round_spe)
        html_string += slippage_df.to_html()
        list_tables.append(slippage_df.to_latex())      
          
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
            html_string += '<h2>' + algo + ': Weekly Slippage Evolution (from FlexStat)</h2>'
            image_name = 'evol_' + algo + '.png'
            h.savefig(FOLDER + image_name)
            list_path.append(FOLDER + image_name)
            list_image.append(image_name)
            html_string += '<img src="cid:%s">\n' %image_name
            
    
    ###########################################################################
    # add figure to email msg.
    ###########################################################################   
    
    # Assume we know that the image files are all in PNG format
    i = 0
    for file in list_path:
        # Open the files in binary mode.  Let the MIMEImage class automatically
        # guess the specific image type.
        fp = open(file, 'rb')
        img = MIMEImage(fp.read())
        img.add_header('Content-ID', '<%s>'%list_image[i])
        fp.close()
        msg.attach(img)
        i += 1
        
    ###########################################################################
    # add the text
    ###########################################################################     
    msg.attach(MIMEText(html_string, 'html'))   
    
         
    ###########################################################################
    # add the pdf file
    ###########################################################################
    latex_doc = latex_string(list_path, list_tables)
        
    tex_file_path   = FOLDER + 'Report_' + day_str
    file_w          = open(tex_file_path + ".tex", "w")
    file_w.write(latex_doc)
    file_w.close()
    
    logging.info(LATEX, "-output-directory="+ FOLDER, tex_file_path + ".tex")
    call([LATEX, "-output-directory="+ FOLDER, tex_file_path + ".tex"], shell=False)
    # 2 compilations in order to build the index correctly
    call([LATEX, "-output-directory="+ FOLDER, tex_file_path + ".tex"], shell=False)
    pdf = MIMEBase('application', "octet-stream")
    
    file_r = open(tex_file_path + '.pdf', "rb")
    pdf.set_payload( file_r.read() )
    Encoders.encode_base64(pdf)
    file_r.close()
    
    pdf.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(tex_file_path + '.pdf'))
    msg.attach(pdf)
             
    ###########################################################################
    # Send the email via our own SMTP server.
    ###########################################################################
    s = smtplib.SMTP('172.29.97.16')
    s.sendmail(msg['From'], to, msg.as_string())
    s.quit()
    #plt.show()

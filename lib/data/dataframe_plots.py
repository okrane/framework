# -*- coding: utf-8 -*-
"""
Created on Wed Sep 18 10:19:20 2013

@author: njoseph
"""

import matplotlib as matplotlib
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import pandas as pd
import datetime as dt
import pytz
import numpy as np
import lib.data.matlabutils as matlabutils

def stackbar(data,
            var=None, # variable name
            gvar=None, #
            gvar_sbucket=None,# used if plot is on specified time bucket
            gvar_tzname='Europe/London',
            is_sort_gvar=True, #
            gvar_vals=None, #
            is_horizontal=False,
            ecart_bucket=0,
            color='b',
            title='',
            xlabel='',
            ylabel='',
            add_gvar_txt=False,
            legend_loc=None,
            cmap=cm.spectral,
            show=True,
            VALS_MULTIPLIER=1.05,
            FIG_SIZE = None,
            alpha = 0.85):
    
    #-----------------------------------
    # TEST input
    #-----------------------------------
    #-- mandatory
    if var is None or gvar is None:
        raise ValueError('mandatory colname is missing')
    
    #-- gvar value check
    if all([isinstance(x,basestring) for x in data[gvar]]):
        is_gvar_datetime=False
    elif all([isinstance(x,dt.datetime) for x in data[gvar]]):
        is_gvar_datetime=True
    else:
        raise ValueError('gvar values should be string or datetime')
    
    #-----------------------------------
    # NEEDED DATA
    #-----------------------------------
    # -- uni_gvar
    if is_gvar_datetime:
        
        def apply2dt(x,tzname):
            if isinstance(x,pd.tslib.Timestamp):
                out=x.to_datetime()
            else:
                out = x
            if out.tzinfo is not None and tzname is not None:
                out=out.astimezone(tz=pytz.timezone(tzname))
            elif out.tzinfo is None:
                # bidouille, pour que le .values soit bin un datetime , il faut qu'il y ait une timezone
                out=pytz.UTC.localize(out)
            return out
        
        data[gvar]=[apply2dt(x,gvar_tzname) for x in data[gvar]]
        
        if gvar_sbucket is None:
            uni_gvar=np.unique(data[gvar].values).tolist()
            if len(uni_gvar)>1:
                first_dt_uni_gvar=uni_gvar[0]-(uni_gvar[1]-uni_gvar[0])
            else:
                first_dt_uni_gvar=uni_gvar[0]-dt.timedelta(seconds=10)
                
        else:
            data[gvar_sbucket]=[apply2dt(x,gvar_tzname) for x in data[gvar_sbucket]]
            uni_=matlabutils.uniqueext(data[[gvar,gvar_sbucket]].values, rows=True)
            uni_gvar=[x[0] for x in uni_]
            uni_gvar_sbucket=[x[1] for x in uni_]
            idx_s=np.argsort(uni_gvar)
            uni_gvar=[uni_gvar[i] for i in idx_s]
            uni_gvar_sbucket=[uni_gvar_sbucket[i] for i in idx_s]
            first_dt_uni_gvar=uni_gvar_sbucket[0]
    else:
        uni_gvar=np.unique(data[gvar].values).tolist()
        if is_sort_gvar:
            uni_gvar = data.groupby(gvar)[var].sum().order().index.values
              
    # -- uni_gvar_vals
    uni_gvar_vals=None
    if gvar_vals is not None:
        uni_gvar_vals=np.unique(data[gvar_vals].values).tolist()
        
    #-----------------------------------
    # PLOT
    #-----------------------------------
    # -- colors and legend  
    if uni_gvar_vals is not None:
        if isinstance(cmap,list):
            colors_gvar_vals = [cmap[int(x%len(cmap))] for x in range(0,len(uni_gvar_vals))]
        else:
            colors_gvar_vals = cmap(np.linspace(0, 1.0, len(uni_gvar_vals)))
        
        uni_gvar_vals_islabeled = np.array([False]*len(uni_gvar_vals))
    else:
        colors_gvar_vals=color
        
    # -- set  
    if FIG_SIZE is not None:
        out = plt.figure(figsize = FIG_SIZE)
    else:
        out = plt.figure()
    axes = plt.gca()
    axes.grid(True)
    plt.hold(True)
    if is_horizontal:
        ylim=[0,len(uni_gvar)]
        xlim=[0,0]
    else:
        xlim=[0,len(uni_gvar)]
        ylim=[0,0]
    
    for idx_gvar in range(0,len(uni_gvar)):
        #--
        this_gvar_data=data[data[gvar]==uni_gvar[idx_gvar]]
        
        #--
        tmp_cum_value=0
        if uni_gvar_vals is not None:
            #----
            # Case with gvar_vals to separate in the bars
            #----
            
            tmp_uni_gvar_vals=np.unique(this_gvar_data[gvar_vals].values).tolist()
            
            for this_gvar_vals in tmp_uni_gvar_vals:
                #--
                idx_gvar_vals=np.nonzero([x==this_gvar_vals for x in uni_gvar_vals])[0][0]
                tmp_data=this_gvar_data[this_gvar_data[gvar_vals]==this_gvar_vals]
                if tmp_data.shape[0]!=1:
                    raise ValueError('gvar_vals is not unique')
                    
                #--
                args=[]
                if not is_horizontal:
                    if not is_gvar_datetime:
                        args.append([idx_gvar+0.5*ecart_bucket,idx_gvar+1-0.5*ecart_bucket,idx_gvar+1-0.5*ecart_bucket,idx_gvar+0.5*ecart_bucket])
                    elif gvar_sbucket is not None:
                        args.append([uni_gvar_sbucket[idx_gvar],uni_gvar[idx_gvar],uni_gvar[idx_gvar],uni_gvar_sbucket[idx_gvar]])
                    elif idx_gvar==0:
                        args.append([first_dt_uni_gvar,uni_gvar[idx_gvar],uni_gvar[idx_gvar],first_dt_uni_gvar])
                    else:
                        args.append([uni_gvar[idx_gvar-1],uni_gvar[idx_gvar],uni_gvar[idx_gvar],uni_gvar[idx_gvar-1]])
                    args.append([tmp_cum_value,tmp_cum_value,tmp_cum_value+tmp_data[var].values[0],tmp_cum_value+tmp_data[var].values[0]])
                else:
                    args.append([tmp_cum_value,tmp_cum_value+tmp_data[var].values[0],tmp_cum_value+tmp_data[var].values[0],tmp_cum_value])
                    if not is_gvar_datetime:
                        args.append([idx_gvar+0.5*ecart_bucket,idx_gvar+0.5*ecart_bucket,idx_gvar+1-0.5*ecart_bucket,idx_gvar+1-0.5*ecart_bucket])
                    elif gvar_sbucket is not None:
                            args.append([uni_gvar_sbucket[idx_gvar],uni_gvar_sbucket[idx_gvar],uni_gvar[idx_gvar],uni_gvar[idx_gvar]])
                    elif idx_gvar==0:
                        args.append([first_dt_uni_gvar,first_dt_uni_gvar,uni_gvar[idx_gvar],uni_gvar[idx_gvar]])
                    else:
                        args.append([uni_gvar[idx_gvar-1],uni_gvar[idx_gvar-1],uni_gvar[idx_gvar],uni_gvar[idx_gvar]])
                tmp_cum_value+=tmp_data[var].values[0]
                
                #--        
                kwargs={'facecolor':colors_gvar_vals[idx_gvar_vals],'alpha': alpha}
                
                if not uni_gvar_vals_islabeled[idx_gvar_vals]:
                    kwargs.update({'label':uni_gvar_vals[idx_gvar_vals]})
                    uni_gvar_vals_islabeled[idx_gvar_vals]=True
                
                #--             
                plt.gca().fill(*args,**kwargs)
        else:
            #----
            # Case with NO gvar_vals to separate in the bars
            #----
            
            if this_gvar_data.shape[0]!=1:
                raise ValueError('gvar is not unique')
                
            #--
                # need to change
            args=[]
            if not is_horizontal:
                if not is_gvar_datetime:
                    args.append([idx_gvar+0.5*ecart_bucket,idx_gvar+1-0.5*ecart_bucket,idx_gvar+1-0.5*ecart_bucket,idx_gvar+0.5*ecart_bucket])
                elif gvar_sbucket is not None:
                    args.append([uni_gvar_sbucket[idx_gvar],uni_gvar[idx_gvar],uni_gvar[idx_gvar],uni_gvar_sbucket[idx_gvar]])
                elif idx_gvar==0:
                    args.append([first_dt_uni_gvar,uni_gvar[idx_gvar],uni_gvar[idx_gvar],first_dt_uni_gvar])
                else:
                    args.append([uni_gvar[idx_gvar-1],uni_gvar[idx_gvar],uni_gvar[idx_gvar],uni_gvar[idx_gvar-1]])
                args.append([0,0,this_gvar_data[var].values[0],this_gvar_data[var].values[0]])
            else:
                if not is_gvar_datetime:
                    args.append([0,this_gvar_data[var].values[0],this_gvar_data[var].values[0],0])
                elif gvar_sbucket is not None:
                        args.append([uni_gvar_sbucket[idx_gvar],uni_gvar_sbucket[idx_gvar],uni_gvar[idx_gvar],uni_gvar[idx_gvar]])
                elif idx_gvar==0:
                    args.append([first_dt_uni_gvar,first_dt_uni_gvar,uni_gvar[idx_gvar],uni_gvar[idx_gvar]])
                else:
                    args.append([uni_gvar[idx_gvar-1],uni_gvar[idx_gvar-1],uni_gvar[idx_gvar],uni_gvar[idx_gvar]])
                args.append([idx_gvar+0.5*ecart_bucket,idx_gvar+0.5*ecart_bucket,idx_gvar+1-0.5*ecart_bucket,idx_gvar+1-0.5*ecart_bucket])
                
            tmp_cum_value+=this_gvar_data[var].values[0]
            
            #--        
            kwargs={'facecolor':colors_gvar_vals,'alpha':0.85}
            
            #--             
            plt.gca().fill(*args,**kwargs)
    
        #-- 
        if is_horizontal:
            xlim[0]=min(xlim[0],tmp_cum_value)
            xlim[1]=max(xlim[1],tmp_cum_value)
            
        else:
            ylim[0]=min(ylim[0],tmp_cum_value)
            ylim[1]=max(ylim[1],tmp_cum_value)
            
        #--
        if add_gvar_txt:
            if is_horizontal:
                plt.text(tmp_cum_value*1.02+2, idx_gvar + 0.33, str(round(tmp_cum_value*100)/100),
                            ha='left', va='bottom', size = 12, weight = 750)       
                            
    plt.hold(False)
    
    #-----------
    #- HANDLE TICKS LABEL and bounds
    #-----------
    
    if not is_gvar_datetime:
        if not is_horizontal:
            ylim[1]*=VALS_MULTIPLIER 
            plt.xticks(np.array(range(0,len(uni_gvar)))+0.5, uni_gvar )
        else:
            xlim[1]*=VALS_MULTIPLIER 
            plt.yticks(np.array(range(0,len(uni_gvar)))+0.5, uni_gvar )
    else:
        if not is_horizontal:
            ylim[1]*=VALS_MULTIPLIER
            xlim=[matplotlib.dates.date2num(first_dt_uni_gvar),matplotlib.dates.date2num(uni_gvar[-1])]
        else:
            xlim[1]*=VALS_MULTIPLIER
            ylim=[matplotlib.dates.date2num(first_dt_uni_gvar),matplotlib.dates.date2num(uni_gvar[-1])]
    
    axes.axis(tuple(xlim+ylim))
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title, size = 'large')
    
    if gvar_vals is not None and legend_loc is not None:
        plt.legend(loc=legend_loc)   
        
    if show:
        plt.show()
        
    return out


def datetime2matlabdn(date):
    if date.tzinfo is not None:
        date=date.replace(tzinfo=None)
    frac_seconds = (date-dt.datetime(date.year,date.month,date.day,0,0,0)).seconds / (24.0 * 60.0 * 60.0)
    frac_microseconds = date.microsecond / (24.0 * 60.0 * 60.0 * 1000000.0)
    return date.toordinal() + frac_seconds + frac_microseconds






if __name__=='__main__':
    from lib.plots.color_schemes import kc_main_colors
    from lib.data.ui.Explorer import Explorer
    from lib.tca.referentialdata import ReferentialDataProcessor
    from lib.tca.marketdata import MarketDataProcessor
    from lib.tca.algostats import AlgoStatsProcessor
    from lib.tca.algodata import AlgoDataProcessor
    import lib.data.dataframe_tools as dftools
    from lib.data.ui.Explorer import Explorer
    from lib.dbtools.connections import Connections
    import pytz
    
    #-----------------------
    #-----  HORIZONTAL SHOW ALL
    #-----------------------
     
    #-----  data create (without gvar_vals)
    sdate=dt.datetime(2013,8,13)
    edate=dt.datetime(2013,8,15)
    seq_data = AlgoStatsProcessor(start_date = dt.datetime(2013,8,12), end_date = dt.datetime(2013,8,14))
    seq_data.get_db_data(level='sequence')
    data=seq_data.data_sequence
    data['is_dma']=data['TargetSubID' ].apply(lambda x: 'Algo DMA' if x in  ['ON1','ON2','ON3'] else 'Other')
    data=dftools.agg(data,group_vars=['strategy_name_mapped','is_dma'],
                       stats={'mturnover_euro': lambda df : np.sum(df.rate_to_euro*df.turnover)*1e-6})
     
    #-- plot
    stackbar(data,var='mturnover_euro',gvar='strategy_name_mapped',gvar_vals='is_dma',
             ecart_bucket=0.2,is_horizontal=True,show=True,add_gvar_txt=True,legend_loc='lower right',
             xlabel='Volume/Algo (,000,000 Euros)',
             title='From ' + dt.datetime.strftime(sdate,'%d/%m/%Y') + ' To '+ dt.datetime.strftime(edate,'%d/%m/%Y') + '\nTotal Turnover: ' + str(round(np.sum(data['mturnover_euro']),1)) + ' MEuros\n DMA Turnover: ' + str(round(np.sum(data['mturnover_euro'][data['is_dma']=='Algo DMA']),1)) + ' MEuros',
             cmap=[kc_main_colors()['blue_1'],kc_main_colors()['blue_2']],
             FIG_SIZE=(13,7))
        
    
    #-----------------------
    #-----  INTRADAY EXEC CURVE
    #-----------------------
     
    #-----  data create (without gvar_vals)
    sdate=dt.datetime(2013,8,14)
    edate=dt.datetime(2013,8,15)
    algo_data = AlgoStatsProcessor(start_date = sdate, end_date = edate)
    algo_data.get_db_data(level='deal')
    algo_data.get_intraday_agg_deals_data()
    data=algo_data.data_intraday_agg_deals
              
    # -- config
    stats_config={'nb_day': lambda df : len(df.mturnover_euro),
                  'mturnover_euro': lambda df : np.sum(df.mturnover_euro)}
      
    # handle timezone
    if sdate.tzinfo is None and data.index[0].to_datetime().tzinfo is not None:
        sdate=pytz.UTC.localize(sdate)
        edate=pytz.UTC.localize(edate)
        # -- do
        data=data[map(lambda x : x.to_datetime()>=sdate and x.to_datetime()<=edate,data.index)]
      
    # -- needed 4 aggregate
    max_day=max([x.to_datetime().date() for x in data.index])
    data['end_slice']=[dt.datetime.combine(max_day,x.timetz()) for x in data.index]
    # data['begin_slice']=[dt.datetime.strftime(dt.datetime.combine(max_day,x.time()),fmt) for x in data['begin_slice']]
      
    # -- aggregate
    data = dftools.agg(data,group_vars=['end_slice','strategy_name_mapped'],stats=stats_config)
     
    #-- plot
    stackbar(data,var='mturnover_euro',gvar='end_slice',gvar_vals='strategy_name_mapped',show=True,legend_loc='upper center')
     

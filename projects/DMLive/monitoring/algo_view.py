import django, django.http
from django.shortcuts import render_to_response
from django.template import Template
from django.core.context_processors import csrf
from django.template import RequestContext

import matplotlib
#matplotlib.use('cairo.png')

import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import numpy as np
from utils.connections import Connections    
from monitoring.forms import AlgoSearchForm
import datetime
from monitoring.cfg import config


def twap_view(order_id, server):
    format = "%d" + ",%d" * (len(config.algo['Twap']['indicator_list'])-1)
    str_ind = format % tuple(config.algo['Twap']['indicator_list'])
    query = """select
    convert(varchar,aind.Date,108) as Time,aind.Microseconds,
    aind.IndicatorId,aparconf.Label,
    aind.TradingVenueId,aind.Value
    from dm_algo..Algo_Indicator aind
    left join dm_algo..Algo_TacticSequence atacseq
    on (
    aind.TacticSequenceId=atacseq.TacticSequenceId  
    and aind.ServerId=atacseq.ServerId 
    )
    left join dm_algo..Algo_Ref_Parameter aparconf
    on (
    aind.IndicatorId=aparconf.ParameterId  
    )
    left join dm_algo..Algo_Tactic atac
    on (
    atacseq.TacticId =atac.TacticId  
    and atacseq.ServerId =atac.ServerId 
    )
    left join dm_algo..Algo_Tactic atac4Rel
    on (
    atac.RelevantTacticId =atac4Rel.TacticId  
    and atac.ServerId =atac4Rel.ServerId 
    )
    left join dm_algo..Algo_Tactic atac4Algo
    on (
    atac.AlgoTacticId =atac4Algo.TacticId  
    and atac.ServerId =atac4Algo.ServerId 
    )
    left join dm_algo..Algo_Order aor 
    on (
    atac.OrderId= aor.OrderId 
    and atac.ServerId = aor.ServerId
    )
    where (
    aor.OrderId='%s' and aor.ServerId=%s and IndicatorId in (%s)
    )
    order by aind.Date,aind.Microseconds""" % (order_id, server, str_ind)
    (data, schema) = Connections.exec_sql_schema(config.database['SERVER'], query)     
    
    fig = Figure()
    rect = fig.patch
    rect.set_facecolor((1, 1, 0.9))
    canvas = FigureCanvas(fig)    
    ax = fig.add_subplot(111)
    today = datetime.date.today()
    
    #### Plot the Execution Curve
    values = [e[schema.index('Value')] for e in data if e[schema.index('Label')] == 'EffectiveExecQty']
    dates_str  = [e[schema.index('Time')] for e in data if e[schema.index('Label')] == 'EffectiveExecQty']
    date_time = [datetime.datetime(today.year, today.month, today.day, int(d[0:2]), int(d[3:5]), int(d[6:8])) for d in dates_str]
    ax.step(date_time, values, linewidth = 2, color = (0, 0, 0))
    
    
    for label in ax.xaxis.get_ticklabels():
    # label is a Text instance
        label.set_color('black')
        label.set_rotation(45)
        label.set_fontsize(8)
    #ax.xlabel('Dates')
    # prepare the response, setting Content-Type
    response=django.http.HttpResponse(content_type='image/png')

    # print the image on the response
    canvas.print_png(response)
    return response


def vwap_view(order_id, server):
    format = "%d" + ",%d" * (len(config.algo['Vwap']['indicator_list'])-1)
    str_ind = format % tuple(config.algo['Vwap']['indicator_list'])
    query = """select
    convert(varchar,aind.Date,108) as Time,aind.Microseconds,
    aind.IndicatorId,aparconf.Label,
    aind.TradingVenueId,aind.Value
    from dm_algo..Algo_Indicator aind
    left join dm_algo..Algo_TacticSequence atacseq
    on (
    aind.TacticSequenceId=atacseq.TacticSequenceId  
    and aind.ServerId=atacseq.ServerId 
    )
    left join dm_algo..Algo_Ref_Parameter aparconf
    on (
    aind.IndicatorId=aparconf.ParameterId  
    )
    left join dm_algo..Algo_Tactic atac
    on (
    atacseq.TacticId =atac.TacticId  
    and atacseq.ServerId =atac.ServerId 
    )
    left join dm_algo..Algo_Tactic atac4Rel
    on (
    atac.RelevantTacticId =atac4Rel.TacticId  
    and atac.ServerId =atac4Rel.ServerId 
    )
    left join dm_algo..Algo_Tactic atac4Algo
    on (
    atac.AlgoTacticId =atac4Algo.TacticId  
    and atac.ServerId =atac4Algo.ServerId 
    )
    left join dm_algo..Algo_Order aor 
    on (
    atac.OrderId= aor.OrderId 
    and atac.ServerId = aor.ServerId
    )
    where (
    aor.OrderId='%s' and aor.ServerId=%s and IndicatorId in (%s)
    )
    order by aind.Date,aind.Microseconds""" % (order_id, server, str_ind)
    (data, schema) = Connections.exec_sql_schema(config.database['SERVER'], query)     
    
    fig = Figure()
    rect = fig.patch
    rect.set_facecolor((1, 1, 0.9))
    canvas = FigureCanvas(fig)    
    ax = fig.add_subplot(111)
    today = datetime.date.today()
    #### Plot the Lower Curve
    values = [e[schema.index('Value')] for e in data if e[schema.index('Label')] == 'MinExecQty']
    dates_str  = [e[schema.index('Time')] for e in data if e[schema.index('Label')] == 'MinExecQty']
    date_time = [datetime.datetime(today.year, today.month, today.day, int(d[0:2]), int(d[3:5]), int(d[6:8])) for d in dates_str]    
    ax.plot(date_time, values, linewidth = 2, color =(0, 0.67, 0.12))
    
    #### Plot the Upper Curve
    values = [e[schema.index('Value')] for e in data if e[schema.index('Label')] == 'MaxExecQty']
    dates_str  = [e[schema.index('Time')] for e in data if e[schema.index('Label')] == 'MaxExecQty']
    date_time = [datetime.datetime(today.year, today.month, today.day, int(d[0:2]), int(d[3:5]), int(d[6:8])) for d in dates_str]
    ax.plot(date_time, values, linewidth = 2, color = (0, 0.67, 0.12))
    
    #### Plot the Middle Curve
    values = [e[schema.index('Value')] for e in data if e[schema.index('Label')] == 'MedExecQty']
    dates_str  = [e[schema.index('Time')] for e in data if e[schema.index('Label')] == 'MedExecQty']
    date_time = [datetime.datetime(today.year, today.month, today.day, int(d[0:2]), int(d[3:5]), int(d[6:8])) for d in dates_str]
    ax.plot(date_time, values, linewidth = 2, color = (1, 0, 0))
    
    #### Plot the Execution Curve
    values = [e[schema.index('Value')] for e in data if e[schema.index('Label')] == 'EffectiveExecQty']
    dates_str  = [e[schema.index('Time')] for e in data if e[schema.index('Label')] == 'EffectiveExecQty']
    date_time = [datetime.datetime(today.year, today.month, today.day, int(d[0:2]), int(d[3:5]), int(d[6:8])) for d in dates_str]
    ax.step(date_time, values, linewidth = 2, color = (0, 0, 0))
    
    
    for label in ax.xaxis.get_ticklabels():
    # label is a Text instance
        label.set_color('black')
        label.set_rotation(45)
        label.set_fontsize(8)
    #ax.xlabel('Dates')
    # prepare the response, setting Content-Type
    response=django.http.HttpResponse(content_type='image/png')

    # print the image on the response
    canvas.print_png(response)
    return response

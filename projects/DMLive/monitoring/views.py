# Create your views here.
import django, django.http
from django.shortcuts import render_to_response
from django.template import Template
from django.core.context_processors import csrf
from django.template import RequestContext

import numpy as np
from monitoring.forms import AlgoSearchForm
from utils.connections import Connections
from monitoring.algo_view import *
from monitoring.cfg import config


def mainform(request):
    """ Main page: 
    * Research form wit multiple options:
        ** By Client Order Id (for traders)
        ** By Strategy and Server (for everyone)
        ** By Order Id: for help guys.
    TODOS: 
    * add server Id to everything?
    * ors id as well?    
    """
        
    if request.method == 'POST': # If the form has been submitted...
        form = AlgoSearchForm(request.POST) # A form bound to the POST data
        if form.is_valid(): # All validation rules pass
            # Process the data in form.cleaned_data
            # ...
            client_id = request.POST['client_order_id']
            strategy  = request.POST['strategy']
            server  = request.POST['server']
            order_id  = request.POST['order_id']
            
            print server
            if client_id:                
                return django.http.HttpResponseRedirect('search_results_by_client_%s' % (client_id))
            elif order_id:
                return django.http.HttpResponseRedirect('search_results_by_order_%s' % (order_id))
            else:
                return django.http.HttpResponseRedirect('search_results_by_strategy_%s_%s' % (strategy, server)) # Redirect after POST
       
    else:
        form = AlgoSearchForm() # An unbound form
        
    return render_to_response('mainform.tmpl', {'form': form,}, context_instance=RequestContext(request) )

def client_overview(request, client):    
    where = """atac.ParentTacticId = NULL   
        and atacseq.ParentTacticSequenceId = NULL        
        and aor.ClientOrderId=%s 
        order by atac.OrderId,atacseq.TacticId,atacseq.TacticSequenceId""" % (client)
    
    query = results_table_head + ' where ' + where        
    (algo_data, algo_heads) = Connections.exec_sql_schema(config.database['SERVER'], query.replace ('\n', ''))
    ## compute total asked quantity: sum of live + canceled + filled
    asked_qty = [e[algo_heads.index('AskedQuantity')] for e in algo_data]
    cum_qty = [e[algo_heads.index('TacticCumQty')] for e in algo_data ]
    total_asked = max(asked_qty) ## wrong computation: change ASAP
    
    # compute the total executed quantity
    deals_query = """
    select TradingDestinationId, DealTime, Microseconds, Quantity, Price, LiquidityIndicator, TradingVenueType, TradingVenueId from dm_algo..Algo_Deal adeal
    where 
    adeal.TacticSequenceId in
    (
    select TacticSequenceId from dm_algo..Algo_TacticSequence 
    where TacticId in
    (
    select distinct TacticId 
    from dm_algo..Algo_Tactic 
    where OrderId in(
    select distinct OrderId from dm_algo..Algo_Order
    where ClientOrderId = %s)
    )
    )
    """ % (client)
    (deals_data, deals_schema) = Connections.exec_sql_schema(config.database['SERVER'], deals_query.replace ('\n', ''))
    
    total_executed = sum([e[deals_schema.index('Quantity')] for e in deals_data])
    #print total_asked, total_executed
    
    
    fig = Figure()
    rect = fig.patch
    rect.set_facecolor((1, 1, 0.9))
    canvas = FigureCanvas(fig)    
    ax = fig.add_subplot(111)    
    ax.pie([total_executed, total_asked - total_executed], labels = ['Executed', 'Remaining'], colors = [(0, 1, 0), (0.5, 0.5, 0.5)], shadow = True)    
    
    response=django.http.HttpResponse(content_type='image/png')
    # print the image on the response
    canvas.print_png(response)
    return response
 
def plot_view(request, order_id, server):
    """ The first picture inside the real_time view
    The function redirects in function of the target strategy towards another function mapped by algo.    
    """
    
    query_tactic_type = "select Name from dm_algo..Algo_Tactic where OrderId = '%s' and ServerId = %s and ParentTacticId = Null" % (order_id, server)
    tactic_type = Connections.exec_sql(config.database['SERVER'], query_tactic_type)[0][0]
    
    # TODO: replace with the mapping for the plots
    
    if tactic_type in config.algo.keys():
        exec "%s(order_id, server)" % config.algo[tactic_type]['mapping']
    else:
        print "Missing Strategy from mapping"

def real_time_view(request, order_id, server):  
    # Sequence View
    query = """select
    aor.ServerId,aor.OrderId,atacseq.TacticId,atacseq.TacticSequenceId,aor.ClientOrderSource,aor.ClientOrderId,    
    aor.SecurityId,aor.Side,aor.OrderDate,aor.TraderId,    
    atac.Sequence as StrategySequence,atac.Name as StrategyName,atac.Type as StrategyType,
    convert(varchar,atac.StartTime,108) as StrategyStart,atac.StartTimeM,convert(varchar,atac.EndTime,108) as StrategyEnd,atac.EndTimeM,    
    atacseq.Sequence,atacseq.RequestType,convert(varchar,atacseq.RequestTime,108),atacseq.RequestTimeM,atacseq.ReplyType,convert(varchar,atacseq.ReplyTime,108),atacseq.ReplyTimeM,
    convert(varchar,atacseq.StartTime,108),atacseq.StartTimeM,convert(varchar,atacseq.EndTime,108),atacseq.EndTimeM,
    atacseq.EventId,atacseq.Status,    
    atacseq.TacticCumQty,atacseq.AskedQuantity,    
    convert(varchar,atacseq.AskedStartTime,108) as AskedStartTime,convert(varchar,atacseq.AskedEndTime,108) as AskedEndTime,
    atacseq.Limit,atacseq.WouldLevel,atacseq.ReferencePrice,atacseq.MinPercentVolume,atacseq.MaxPercentVolume,atacseq.FlagSOR,
    atacseq.PrimaryVolume,atacseq.ExecutionStyle,atacseq.ExcludeAuctions
    from dm_algo..Algo_Tactic atac
    left join dm_algo..Algo_TacticSequence atacseq
    on (
    atacseq.TacticId = atac.TacticId 
    and atacseq.ServerId = atac.ServerId
    )
    left join dm_algo..Algo_Order aor
    on (
    atac.OrderId = aor.OrderId 
    and atac.ServerId = aor.ServerId
    )
    where     
    atac.ParentTacticId = NULL   
    and atacseq.ParentTacticSequenceId = NULL 
    and aor.OrderId='%s' and aor.ServerId=%s
    order by atac.OrderId,atacseq.TacticId,atacseq.TacticSequenceId""" % (order_id, server)
    (algo_data, algo_heads) = Connections.exec_sql_schema(config.database['SERVER'], query.replace ('\n', ''))
    
    extra_query = """select aparconf.Label,atacseqparam.* 
    from dm_algo..Algo_Order aor
    left join dm_algo..Algo_Tactic atac
    on (
    atac.OrderId = aor.OrderId 
    and atac.ServerId = aor.ServerId
    )
    left join dm_algo..Algo_TacticSequence atacseq
    on (
    atacseq.TacticId = atac.TacticId 
    and atacseq.ServerId = atac.ServerId
    )
    left join dm_algo..Algo_TacticSequenceParam atacseqparam
    on (
    atacseq.TacticSequenceId=atacseqparam.TacticSequenceId
    and atacseq.ServerId=atacseqparam.ServerId
    )
    left join dm_algo..Algo_Ref_Parameter aparconf
    on (
    atacseqparam.ParameterId=aparconf.ParameterId  
    )
    where
    aor.OrderId='%s' and aor.ServerId=%s 
    and atacseqparam.TacticSequenceId = atacseq.AlgoTacticSequenceId
    order by atacseqparam.TacticSequenceId""" % (order_id, server)
    (extra_data, extra_heads) = Connections.exec_sql_schema(config.database['SERVER'], extra_query.replace ('\n', ''))
    for i in range(len(algo_data)):
        algo_data[i] = list(algo_data[i])
    
    first = True
    for seq in algo_data:
        sequence_name = seq[algo_heads.index('TacticSequenceId')]
        for e in [s for s in extra_data if s[extra_heads.index('TacticSequenceId')] == sequence_name]:
            if first:
                algo_heads.append(e[extra_heads.index('Label')])
            seq.append(e[extra_heads.index('ParameterValue')])
        first = False
    # Deals And Placement View
        
    # Indicator View: Volume Curves
    
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
    
    curves_h = ['Time', 'MinExecQty', 'MedExecQty', 'MaxExecQty', 'EffectiveExecQty']
    curves_e = []
    entry = ['', '', '', '', '' ]
    for e in data:                
        if entry and entry[0] != e[schema.index('Time')]:
            curves_e.append(entry)
            entry = ['', '', '', '', '' ]         
        entry[0] = e[schema.index('Time')]
        entry[curves_h.index(e[schema.index('Label')])] = e[schema.index('Value')]
    
    return render_to_response('indicator_view.tmpl', {'t': algo_data, 
                                                      'h': algo_heads, 
                                                      'id': order_id, 
                                                      's': server, 
                                                      'curves_h': curves_h,
                                                      'curves_e': curves_e}, context_instance=RequestContext(request) )
    

results_table_head = """select
    aor.OrderId,atac.Name as StrategyName, atacseq.TacticSequenceId,aor.ClientOrderId,
    aor.SecurityId,aor.Side,aor.OrderDate,aor.TraderId,    
    convert(varchar,atac.StartTime,108) as StrategyStart,convert(varchar,atac.EndTime,108) as StrategyEnd,
    atacseq.TacticCumQty,atacseq.AskedQuantity
    from dm_algo..Algo_Tactic atac
    left join dm_algo..Algo_TacticSequence atacseq
    on (
    atacseq.TacticId = atac.TacticId 
    and atacseq.ServerId = atac.ServerId
    )
    left join dm_algo..Algo_Order aor
    on (
    atac.OrderId = aor.OrderId 
    and atac.ServerId = aor.ServerId
    )"""


def search_results_by_client(request, client):
    where = """atac.ParentTacticId = NULL   
        and atacseq.ParentTacticSequenceId = NULL        
        and aor.ClientOrderId=%s 
        order by atac.OrderId,atacseq.TacticId,atacseq.TacticSequenceId""" % (client)
    
    query = results_table_head + ' where ' + where        
    (algo_data, algo_heads) = Connections.exec_sql_schema(config.database['SERVER'], query.replace ('\n', ''))
    server = 7    
    return render_to_response('client_view.tmpl', {'t': algo_data, 'h': algo_heads, 's': server, 'client': client}, context_instance=RequestContext(request) )
    
def search_results_by_order(request, order):
    where = """atac.ParentTacticId = NULL   
        and atacseq.ParentTacticSequenceId = NULL        
        and aor.OrderId=%s 
        order by atac.OrderId,atacseq.TacticId,atacseq.TacticSequenceId""" % (order)
    
    query = results_table_head + ' where ' + where    
        
    (algo_data, algo_heads) = Connections.exec_sql_schema(config.database['SERVER'], query.replace ('\n', ''))    
    server = 7    
    return render_to_response('search_results.tmpl', {'t': algo_data, 'h': algo_heads, 's': server}, context_instance=RequestContext(request) )


def search_results_by_strategy(request, strategy, server):
    if strategy != '*' and server != '*':
        where = """atac.ParentTacticId = NULL   
            and atacseq.ParentTacticSequenceId = NULL
            and atac.Name = '%s'
            and aor.ServerId=%s
            order by atac.OrderId,atacseq.TacticId,atacseq.TacticSequenceId""" % (strategy, server)
        
    elif strategy == '*':
        where = """atac.ParentTacticId = NULL   
            and atacseq.ParentTacticSequenceId = NULL            
            and aor.ServerId=%s
            order by atac.OrderId,atacseq.TacticId,atacseq.TacticSequenceId""" % (server)
        
    query = results_table_head + ' where ' + where   
    
    (algo_data, algo_heads) = Connections.exec_sql_schema(config.database['SERVER'], query.replace ('\n', ''))
    return render_to_response('search_results.tmpl', {'t': algo_data, 'h': algo_heads, 's': server}, context_instance=RequestContext(request) )
    


def search_results(request, client, strategy, server):
    
    query = """select
        aor.OrderId,atacseq.TacticId,atacseq.TacticSequenceId,aor.ClientOrderId,
        aor.SecurityId,aor.Side,aor.OrderDate,aor.TraderId,
        atac.Sequence as StrategySequence,atac.Name as StrategyName,
        convert(varchar,atac.StartTime,108) as StrategyStart,convert(varchar,atac.EndTime,108) as StrategyEnd,
        atacseq.Sequence,atacseq.RequestType,convert(varchar,atacseq.RequestTime,108),atacseq.RequestTimeM,atacseq.ReplyType,convert(varchar,atacseq.ReplyTime,108),atacseq.ReplyTimeM,
        atacseq.TacticCumQty,atacseq.AskedQuantity,
        atacseq.Limit,atacseq.WouldLevel,atacseq.ReferencePrice,atacseq.MinPercentVolume,atacseq.MaxPercentVolume,atacseq.FlagSOR,
        atacseq.PrimaryVolume,atacseq.ExecutionStyle,atacseq.ExcludeAuctions
        from dm_algo..Algo_Tactic atac
        left join dm_algo..Algo_TacticSequence atacseq
        on (
        atacseq.TacticId = atac.TacticId 
        and atacseq.ServerId = atac.ServerId
        )
        left join dm_algo..Algo_Order aor
        on (
        atac.OrderId = aor.OrderId 
        and atac.ServerId = aor.ServerId
        )
        where 
        atac.ParentTacticId = NULL   
        and atacseq.ParentTacticSequenceId = NULL
        and atac.Name = '%s'
        and aor.ServerId=%s
        order by atac.OrderId,atacseq.TacticId,atacseq.TacticSequenceId""" % (strategy, server)
    (algo_data, algo_heads) = Connections.exec_sql_schema(config.database['SERVER'], query.replace ('\n', ''))    
    if algo_data:
        return render_to_response('search_results.tmpl', {'t': algo_data, 'h': algo_heads, 's': server}, context_instance=RequestContext(request) )
    else:
        pass
    
     

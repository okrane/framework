# -*- coding: utf-8 -*-
"""
Created on Mon May 20 13:52:46 2013

@author: njoseph
"""

import pandas as pd
from datetime import *
import pytz
import numpy as np
from lib.data.matlabutils import *
from lib.dbtools.connections import Connections
from lib.logger import *
from lib.io.toolkit import get_traceback


#------------------------------------------------------------------------------
# convert_symbol
#------------------------------------------------------------------------------
def convert_symbol(**kwargs):
    """ Converts between:  (security_id, security_name, ric, glid, bloomberg, sedol, SYMBOL1, SYMBOL2, SYMBOL3, SYMBOL4, SYMBOL5, SYMBOL6)
        the keys to be passed are :
        a. a key < value > containing the value to convert
        b. a key < source > with the type of the element in the above list
            with the value equal to the requested value to convert
        c. a key < dest > containing the target key to convert from the above list.
        d. optionally pass the EXCHGID for disambiguation purposes
        
        @return the requested symbol in a string format (empty string if not found)
        
        Examples:
            print "security_id(%s)->glid=%s"% (110, convert_symbol(source = 'security_id', dest = 'glid', value = 110, exchgid='SEPA'))
            print "security_id(%s)->SECID=%s"% (110, convert_symbol(source = 'security_id', dest = 'SECID', value = 110, exchgid='SEPA'))
            print "security_id(%s)->ISIN=%s"% (110, convert_symbol(source = 'security_id', dest = 'ISIN', value = 110, exchgid='SEPA'))
            print "security_id(%s)->bloomberg=%s"% (110, convert_symbol(source = 'security_id', dest = 'bloomberg', value = 110, exchgid='SEPA'))    
    """   
    ##############################################################
    # input handling: allowed keys
    ##############################################################
    fields = {'security_id': 'SYMBOL6',
              'security_name': "SECNAME",
              'SECNAME': 'SECNAME',
              'sec_id': 'SECID',
              'SECID': 'SECID',
              'SYMBOL6': 'SYMBOL6',
              'flex_id': 'flex_id',
              'bloomberg': 'SYMBOL5',
              'ric': 'SYMBOL4',
              'reuters': 'SYMBOL4',
              'isin': 'SYMBOL2',
              'ISIN': 'SYMBOL2',
              'gl_id': 'SYMBOL1',
              'glid': 'SYMBOL1',
              'sedol': 'SYMBOL3',
              'SYMBOL5': 'SYMBOL5',
              'SYMBOL4': 'SYMBOL4',
              'SYMBOL3': 'SYMBOL3',
              'SYMBOL2': 'SYMBOL2',
              'SYMBOL1': 'SYMBOL1',              
              }
    ##############################################################
    # input handling: manage errors
    ##############################################################
    if 'source' not in kwargs.keys():
        raise NameError('get_repository:convert_symbol - Bad input : "from" paramter is missing')

    if 'dest' not in kwargs.keys():
        raise NameError('get_repository:convert_symbol - Bad input : "to" paramter is missing')    
    
    if 'value' not in kwargs.keys():
        raise NameError('get_repository:convert_symbol - Bad input : "value" paramter is missing')    
    
    if kwargs['source'] not in fields.keys(): 
        raise ValueError('get_repository:convert_symbol - Bad input : unknown symbol type <%s>' % kwargs['source'])    
    
    if kwargs['dest'] not in fields.keys(): 
        raise ValueError('get_repository:convert_symbol - Bad input : unknown symbol type <%s>' % kwargs['dest'])    
    
    # ----------------
    # request
    # ----------------  
    query = "SELECT distinct %s from SECURITY where %s = '%s'" % (fields[kwargs['dest']], fields[kwargs['source']], kwargs['value'])
    query += " and EXCHGID = '%s'" % kwargs['exchgid'] if kwargs.has_key('exchgid') else ""    
    print query
    val=Connections.exec_sql('KGR',query,schema = False)    
    return val[0][0] if len(val) == 1 else val

def currency(**kwargs):  
    """
        Use Example:
        print currency(security_id = 2) 
    """
    ##############################################################
    # input handling
    ############################################################## 
    if not "security_id" in kwargs.keys():
        raise NameError('get_repository:currency - Bad input : security_id is missing')
   
    ##############################################################
    # request and format
    ##############################################################   
    main_destination = exchangeidmain(security_id = kwargs['security_id'])        
    query = "select CCY from SECURITY where SYMBOL6 = %d and STATUS = 'A' and EXCHGID = (select EXCHGID from EXCHANGEREFCOMPL where EXCHANGE = %d) " % (kwargs['security_id'], main_destination)    
    out = Connections.exec_sql('KGR', query, as_dict = True)    
    return out[0]['CCY']

def get_symbol6_from_ticker(ticker):
    ticker.replace('|', ' ')
    if ticker[-3:] == '.AG':
        parent_code = ticker[:-3]
        query = """SELECT distinct SYMBOL6
                FROM SECURITY
                where PARENTCODE='%s'
                and SYMBOL6 <> 'NULL'""" % (parent_code)
    else:
        l = ticker.split('.')
        suffix = l.pop()
        symbol1 = ticker[:-(len(suffix)+1)]

        query = """SELECT distinct sec.SYMBOL6
                FROM SECURITY sec,
                EXCHANGEMAPPING exhm,
                FlextradeExchangeMapping exflex
                where sec.SYMBOL1='%s'
                and sec.EXCHGID=exhm.EXCHGID
                and exflex.SUFFIX='%s'
                and exhm.EXCHANGE=exflex.EXCHANGE
                AND sec.SYMBOL6 <> 'NULL'""" % (symbol1, suffix)

    result = Connections.exec_sql('KGR', query, as_dict = True)
    if len(result) == 0:
        logging.info(query)
        logging.warning("Got no SYMBOL 6 for this ticker: " + ticker)
        return ""
    elif len(result)>1:
        logging.info(query)
        logging.warning("Got more than one SYMBOL 6 for this ticker: " + ticker)
        return result[0]['SYMBOL6']
    else:
        return result[0]['SYMBOL6']
        


#------------------------------------------------------------------------------
# exchangeidmain
#------------------------------------------------------------------------------
def exchangeidmain(**kwargs):
    ##############################################################
    # input handling
    ##############################################################
    #---- exchange_id
    if "security_id" in kwargs.keys():
        lids=kwargs["security_id"]
        if isinstance(lids,list):
            lids=np.array(lids)
        elif not isinstance(lids,np.ndarray):
            lids=np.array([lids])
    else:
        raise NameError('get_repository:exchangeidmain - Bad input : security_id is missing')
    
    out=np.array([np.nan]*lids.size) 
    ##############################################################
    # request and format
    ##############################################################    
    data=exchangeinfo(security_id=lids)
    if data.shape[0]>0:
        data=data[data['EXCHANGETYPE']=='M']
        for i in range(0,data.shape[0]):
            out[np.nonzero(lids==data['security_id'].values[i])[0]]=data['EXCHANGE'].values[i]
    return out

#------------------------------------------------------------------------------
# tradingtime
#------------------------------------------------------------------------------
def tradingtime(**kwargs):
    
    ##############################################################
    # input handling
    ##############################################################
    #---- security_id
    if "security_id" in kwargs.keys():
        lids=kwargs["security_id"]
    else:
        raise NameError('get_repository:exchangeidmain - Bad input : security_id is missing')
    #---- date
    if "date" in kwargs.keys():
        date=kwargs["date"]
    else:
        date=[]
        
    out=pd.DataFrame()
    
    ##############################################################
    # request data
    ##############################################################
    pref_ = "LUIDBC01_" if Connections.connections == "dev" else  ""
    data=exchangeinfo(security_id=lids)
    if (data.shape[0]==0) or (not np.any(data['EXCHANGETYPE']=='M')):
        return out
    data=data[data['EXCHANGETYPE']=='M']
        
    req=("SELECT * "
    " FROM %sKGR..trading_hours_master " 
    " WHERE trading_destination_id = %d "
    " AND context_id is null "
    " AND quotation_group = '%s'") % (pref_,data['EXCHANGE'].values[0],data['quotation_group'].values[0]) 
    
    vals=Connections.exec_sql('KGR',req,schema = True)
    
    if not vals[0]:
        req=('SELECT * '
            ' FROM %sKGR..trading_hours_master ' 
            ' WHERE trading_destination_id = %d '
            ' AND context_id is null '
            ' AND quotation_group is null') % (pref_,data['EXCHANGE'])
        vals=Connections.exec_sql('KGR',req,schema = True)
        if not vals[0]:
            raise NameError('get_repository:tradingtime - No trading hours/ exchange: '+str(data['EXCHANGE'].values[0])+' / quotation_group: '+data['quotation_group'].values[0])
    
    ##############################################################
    # transform data
    ##############################################################  
    out=pd.DataFrame.from_records(vals[0],columns=vals[1])
    out['security_id']=lids
    colnumns_date=['opening_auction','opening_fixing','opening','intraday_stop_auction','intraday_stop_fixing','intraday_stop','intraday_resumption_auction',
                 'intraday_resumption_fixing','intraday_resumption','closing_auction','closing_fixing','closing','post_opening','post_closing',
                 'trading_at_last_opening','trading_at_last_closing','trading_after_hours_opening','trading_after_hours_closing']
    # timezone
    for cols in colnumns_date:
        if (out[cols].values[0] is not None):
            tz=pytz.timezone(data['TIMEZONE'].values[0])
            out[cols]=tz.localize(datetime.combine(datetime(2000,1,1).date(),datetime.strptime(out[cols].values[0][0:8],'%H:%M:%S').time())).timetz()
            if not (date==[]):
                # if date, the output will be in datetime whether in time format
                out[cols]=datetime.combine(datetime.strptime(date, '%d/%m/%Y').date(),out[cols].values[0])

    return out

#------------------------------------------------------------------------------
# exchangeinfo
#------------------------------------------------------------------------------
def exchangeinfo(**kwargs):
    ##############################################################
    # input handling
    ##############################################################
    #---- exchange_id
    if "security_id" in kwargs.keys():
        lids=kwargs["security_id"]
        if isinstance(lids,list):
            lids=np.array(lids)
        elif not isinstance(lids,np.ndarray):
            lids=np.array([lids])
    else:
        raise NameError('get_repository:exchangeinfo - Bad input : security_id is missing')
    
    ##############################################################
    # request and format
    ##############################################################
    # ----------------
    # NEEDED
    # ----------------
    pref_ = "LUIDBC01_" if Connections.connections == "dev" else  ""   
    str_lids="("+"".join([str(x)+',' for x in uniqueext(lids)])
    str_lids=str_lids[:-1]+")"
    # ----------------
    # request
    # ----------------
    req=(" SELECT  sm.security_id,sm.ranking,sm.quotation_group, "
     " erefcompl.EXCHANGE,erefcompl.EXCHGID,erefcompl.MIC,erefcompl.EXCHANGETYPE,erefcompl.TIMEZONE, "
     " gzone.NAME as GLOBALZONE,ex.EXCHGNAME "
     " FROM  security_market sm "
     " LEFT JOIN EXCHANGEREFCOMPL erefcompl ON ( "
     " sm.trading_destination_id=erefcompl.EXCHANGE  "
     " )  "
     " LEFT JOIN GLOBALZONE gzone ON ( "
     " erefcompl.GLOBALZONEID=gzone.GLOBALZONEID  "
     " )  "
     " LEFT JOIN EXCHANGE ex ON ( "
     " erefcompl.EXCHGID=ex.EXCHGID  "
     " )  "
     " WHERE  sm.security_id in %s "
     " ORDER BY sm.security_id,sm.ranking ") % (str_lids)
    
    vals=Connections.exec_sql('KGR',req,schema = True)
    
    return pd.DataFrame.from_records(vals[0],columns=vals[1])
 
#------------------------------------------------------------------------------
# exchangeid2tz
#------------------------------------------------------------------------------
def exchangeid2tz(**kwargs):
    ##############################################################
    # input handling
    ##############################################################
    #---- exchange_id
    if "exchange_id" in kwargs.keys():
        lids=kwargs["exchange_id"]
        if isinstance(lids,list):
            lids=np.array(lids)
        elif not isinstance(lids,np.ndarray):
            lids=np.array([lids])
    else:
        raise NameError('get_repository:exchangeid2tz - Bad input : exchange_id is missing')
        
    out=np.array([None]*lids.size)    
    
    ##############################################################
    # request and format
    ##############################################################
    # ----------------
    # NEEDED
    # ----------------
    pref_ = "LUIDBC01_" if Connections.connections == "dev" else  ""   
    str_lids="("+"".join([str(x)+',' for x in uniqueext(lids)])
    str_lids=str_lids[:-1]+")"
    # ----------------
    # request
    # ----------------
    req=(" SELECT "
        " EXCHANGE,TIMEZONE " 
        " FROM " 
        " EXCHANGEREFCOMPL "
        " WHERE "
        " EXCHANGE in %s ") % (str_lids)
    vals=Connections.exec_sql('KGR',req)
    # ----------------
    # format
    # ----------------        
    if not (not vals):
        for x in vals:
            out[np.where(lids==int(x[0]))[0]]=x[1] 
    return out

#------------------------------------------------------------------------------
#  tdidch2exchangeid
#------------------------------------------------------------------------------
def tdidch2exchangeid(**kwargs):
    ##############################################################
    # input handling
    ##############################################################
    #---- sec_id
    if "security_id" in kwargs.keys():
        secid=kwargs["security_id"]
    else:
        secid=[]
    #---- td_id
    if "td_id" in kwargs.keys():
        lids=kwargs["td_id"]
        if isinstance(lids,list):
            lids=np.array(lids)
        elif not isinstance(lids,np.ndarray):
            lids=np.array([lids])
    else:
        raise NameError('get_repository:tdidch2exchangeid - Bad input : td_id is missing')
        
    out=np.array([None]*lids.size)    
    
    ##############################################################
    # request and format
    ##############################################################
    # ----------------
    # NEEDED
    # ----------------
    pref_ = "LUIDBC01_" if Connections.connections == "dev" else  ""
    str_lids="("+"".join([str(x)+',' for x in uniqueext(lids)])
    str_lids=str_lids[:-1]+")"
    # ----------------
    # PHASE 1 : request on destination
    # ----------------
    req=(" SELECT "
    " trading_destination_id,EXCHANGE "
    " FROM "
    " EXCHANGEMAPPING "
    " WHERE "
    " trading_destination_id in %s ") % (str_lids)

    vals=Connections.exec_sql('KGR',req)
    if not (not vals):
        for x in vals:
            out[np.where(lids==int(x[0]))[0]]=x[1]   
            
    # ----------------
    # PHASE 2 : if there is destination that we couldn't map
    # ----------------
    # Y a-t-il des None dans out ? 
    # Si oui vérifions qu'il s'agit de données US :
    # - oui + on a donné en entrée un security_id-> out = destination primaire
    # - non -> on place un signe '-' devant la trading destination.
    if any(x is None for x in out) and (not all(x is None for x in out)):
        idxnotnone=np.nonzero([x is not None for x in out])[0]
        
        str_exchnotnone="("+"".join([str(x)+',' for x in uniqueext(out[idxnotnone])])
        str_exchnotnone=str_exchnotnone[:-1]+")"
        
        req=(" SELECT "
        " EXCHANGE,TIMEZONE " 
        " FROM " 
        " %sKGR..EXCHANGEREFCOMPL "
        " WHERE "
        " EXCHANGE in %s ") % (pref_,str_exchnotnone)
        vals=Connections.exec_sql('KGR',req)
        
        if (any([x[1] is 'America/New_York' for x in vals])) and (not secid==[]):
            
            # get primary destination
            req=(" SELECT "
            " trading_destination_id " 
            " FROM " 
            " %sKGR..security_market "
            " WHERE "
            " security_id = %d "
            " and ranking=1 ") % (pref_,secid)
            vals=Connections.exec_sql('KGR',req)
            out=np.array([vals[0][0]]*lids.size) 
        else:
            idxnone=np.nonzero([x is None for x in out])[0]
            out[idxnone]=-1*lids[idxnone]
          
    return out

#------------------------------------------------------------------------------
# local_tz_from
#------------------------------------------------------------------------------
def local_tz_from(**kwargs):
    
    out=[]
    
    #### Build the request
    if "security_id" in kwargs.keys():  
        
        # get/transform inputs
        lids=kwargs["security_id"]
        if  isinstance(lids,list):
            lids=np.array(lids)
        elif not isinstance(lids,np.ndarray):
            lids=np.array([lids])
            
        out=np.array([None]*lids.size)

        # construct request
        str_lids="("+"".join([str(x)+',' for x in lids])
        str_lids=str_lids[:-1]+")"
        pref_ = "LUIDBC01_" if Connections.connections == "dev" else  ""
        
        req=(" select "
        " sec.SYMBOL6, exch.TIMEZONE "
        " from "
            " %sKGR..SECURITY sec ,"
            " %sKGR..EXCHANGEREFCOMPL exch "
        " where "
            " sec.SYMBOL6 in %s "
            " and sec.EXCHGID=exch.EXCHGID "
            " and exch.EXCHANGETYPE='M' "
            " and sec.STATUS='A' ") % (pref_,pref_,str_lids)
            
    else:
        raise NameError('get_repository:tz_from - Bad input data')
        
    #### EXECUTE REQUEST 
    vals=Connections.exec_sql('KGR',req)
     
    ####  OUTPUT 
    if not vals:
        return out   
        
    for x in vals:
        out[np.where(lids==int(x[0]))[0]]=x[1]
    
    return out

#------------------------------------------------------------------------------
# matlab 'trading-destination'
#------------------------------------------------------------------------------ 
#case { 'trading-destination', 'trading_destination', 'trading_destination_id', 'td', 'tdi' }
#        %<* Get EXCHANGE from table EXCHANGEMAPPING
#        repository = st_version.bases.repository;
#        code = varargin{1};
#        if ischar(code)
#            opt = options( { 'source', nan}, varargin(2:end));
#            source = opt.get('source');
#            if ischar(source)
#                error('get_repository:trading_destination', 'Argument source should be an integer');
#            elseif isnan(source)
#                res = exec_sql( 'KGR', sprintf( ...
#                    [ 'select sm.trading_destination_id,ss.source_id,td.EXCHGNAME, sm.security_id',...
#                    ' from ',repository,'..security_market sm, ',repository,'..security_source ss ,',...
#                    repository,'..EXCHANGEMAPPING map,',repository,'..EXCHANGE td'...
#                    ' where sm.security_id=ss.security_id  ' ...
#                    ' and  sm.trading_destination_id = map.EXCHANGE' ...
#                    ' and map.EXCHGID = td.EXCHGID' ...
#                    ' and ss.reference = ''%s'' ' ...
#                    ' group by sm.trading_destination_id, ss.source_id,td.EXCHGNAME, sm.security_id,sm.ranking',...
#                    ' order by sm.ranking,sm.trading_destination_id asc'], ...
#                    code));
#                if isempty(res)
#                    error('get_repository:no_result', 'REPOSITORY: no such a code');
#                end
#                if length(unique( cell2mat(res(:,4))))~=1
#                    sources = unique(cell2mat(res(:,2)));
#                    sources = sprintf('%d;', sources);
#                    error('get_repository:ambiguity', 'REPOSITORY: more than one security id for this code, please specify your source (%s)', ...
#                        sources(1:end-1));
#                end
#                sid = unique( cell2mat( res(:,4)));
#                res = cell2mat( res(:,1));
#            else
#                res = exec_sql( 'KGR', sprintf( ...
#                    [ 'select sm.trading_destination_id,sm.security_id from ' ...
#                    ' ',repository,'..security_source ss, ',repository,'..security_market sm' ...
#                    ' where   ss.source_id = %d ' ...
#                    ' and ss.reference = ''%s'' ' ...
#                    ' and sm.security_id = ss.security_id ' ...
#                    ' order by sm.ranking, sm.trading_destination_id asc '], ...
#                    source,code));
#                if isempty(res)
#                    error('get_repository:no_result', 'REPOSITORY: no such a code on <%s>', source);
#                end
#                sid = unique(cell2mat(res(:,2)));
#                if length(sid)~=1
#                    error('get_repository:ambiguity', 'REPOSITORY: more than one security id for this code, please stop this!');
#                end
#                res = cell2mat(res(:,1));
#            end
#        elseif isnumeric(code)
#            res = exec_sql( 'KGR', sprintf(['select trading_destination_id',...
#                ' from ',repository,'..security_market where security_id=%d',...
#                ' order by ranking, trading_destination_id asc'], code));
#            if isempty( res)
#                error(['get_repository:no_result', 'REPOSITORY: no such security_id : ',...
#                    '<%d> in ',repository,'..security_market'], code);
#            end
#            sid = code;
#            res = cell2mat(res);
#        else
#            error('get_repository:code', 'BAD_USE: code must be numeric or char');
#        end
#        varargout = { res, sid};
#        %>*
    
#------------------------------------------------------------------------------
# matlab 'td_info'
#------------------------------------------------------------------------------    
#        repository = st_version.bases.repository;
#        opt = options({'trading_destination_id',[]},varargin(2:end));
#        if isempty(opt.get('trading_destination_id'))
#            [td, sec_id] = get_repository('trading-destination', varargin{1});
#        else
#            td = opt.get('trading_destination_id');
#            sec_id = varargin{1};
#        end
#        
#        in_txt = sprintf('%d,', td);
#        in_txt = sprintf(' in (%s) ', in_txt(1:end-1));
#        
#        td_info = exec_sql('KGR', ...
#            sprintf(['select' ...
#            ' em.place_id "place timezone/id",' ...
#            ' /*em.execution_market_id,*/null,' ...
#            ' em.EXCHGID,' ...
#            ' ex_ref_comp.TIMEZONE,' ...
#            ' em.EXCHANGE,' ...
#            ' /*fzone.zone*/null, ' ...
#            ' /*fzone.zone_suffix*/ (case when ex_ref_comp.GLOBALZONEID = 2 then ''_ameri'' else case when ex_ref_comp.GLOBALZONEID = 3 then ''_asia'' else '''' end end ),' ...
#            ' /*fzone.use_localtime*/0,' ...
#            ' /*fzone.default_timezone*/null,' ...
#            ' sm.ranking'...
#            ' from  ' ...
#            ' ',repository,'..EXCHANGEREFCOMPL ex_ref_comp,'...
#            ' ',repository,'..EXCHANGEMAPPING em' ...
#            ' left outer join ',repository,'..security_market sm',...
#            ' on sm.trading_destination_id = em.EXCHANGE and sm.security_id = ',num2str(sec_id),...
#            ' where em.EXCHANGE  %s' ...
#            ' and ex_ref_comp.EXCHANGE = em.EXCHANGE' ...
#            ' group by em.place_id,em.EXCHGID,ex_ref_comp.TIMEZONE,em.EXCHANGE,ex_ref_comp.GLOBALZONEID,sm.ranking' ...
#            ' order by isnull(sm.ranking,1e6),em.EXCHANGE'], ...
#            in_txt));
#        
#        
#        available_td = ismember(td, [td_info{:, 5}]);
#        if ~all(available_td)
#            warning('get_repository:tdinfo', 'Not enough information available for trading_destination_id: \n\t%sAs a consequence these trading_destinations wont be used', sprintf('<%d>\n\t', td(~available_td)));
#        end
#        td = td(available_td);
#        % < On ordonne td_info suivant le ranking que l'on a recupere grace
#        % a l'appel a [td, sec_id] = get_repository('trading-destination',
#        % varargin{:});
#        %         A = [ 1 25 3 5 27 7 9 10 15 13];
#        [~, idx] = sort(td);
#        [~, idx] = sort(idx);
#        td_info = td_info(idx, :);
#        % all([td_info{:, end}]==td)
#        % >
#        quotation_group = cell(length(td), 1);
#        available_td = true(length(td), 1);
#        price_scale = cell(length(td), 1);
#        for i = 1 : length(td)
#            % kepchTODO: Recuperation des tailles de tick.
#            tmp = exec_sql('KGR',...
#                sprintf(['select quotation_group, /*price_scale_id*/null ' ...
#                ' from ',repository,'..security_market' ...
#                ' where security_id = %d  ' ...
#                ' and trading_destination_id = %d ' ...
#                ' order by ranking,trading_destination_id asc'], sec_id, td(i)));
#            if isempty(tmp)
#                quotation_group{i} = 'null';
#            else
#                quotation_group{i} = tmp(:, 1);
#            end
#            
#%             if isfinite(tmp{:,2})
#%                 price_scale{i} = cell2mat(exec_sql('BSIRIUS',...
#%                     sprintf('select threshold, tick_size from repository..price_scale_rule_threshold where price_scale_id = %d order by threshold asc', tmp{:,2})));
#%             end
#            available_td(i) = ~isempty(quotation_group{i});
#        end
#        if ~all(available_td)
#            warning('get_repository:tdinfo', 'No quotation_group for security_id : <%d> and trading_destination_id: \n\t%s', sec_id, sprintf('<%d>\n\t', td(~available_td)));
#        end
#        td = td(available_td);
#        quotation_group = cellflat(quotation_group(available_td))';
#        td_info = td_info(available_td, :);
#        if ~isempty(td_info)
#            varargout = { struct('place_id', td_info(:,1), 'trading_destination_id', num2cell(td(:)), 'execution_market_id', td_info(:,2), ...
#                'short_name', td_info(:,3), 'timezone', td_info(:,4), 'quotation_group', quotation_group, ...
#                'global_zone_name', td_info(:,6), ...
#                'global_zone_suffix', td_info(:,7), ...
#                'localtime', td_info(:,8), ...
#                'default_timezone', td_info(:,9), ...
#                'price_scale', price_scale)};
#        else
#            error('get_repository:tdinfo', 'REPOSITORY:No trading destination with enough information to work with it!');
#        end    
    
    
if __name__ == "__main__":
    print get_symbol6_from_ticker("MMK.VN")
    print get_symbol6_from_ticker("PSMd.AG")
    print exchangeinfo(security_id = 2)    
    
    print "security_id(%s)->glid=%s"% (110, convert_symbol(source = 'security_id', dest = 'glid', value = 110, exchgid='SEPA'))
    print "security_id(%s)->SECID=%s"% (110, convert_symbol(source = 'security_id', dest = 'SECID', value = 110, exchgid='SEPA'))
    print "security_id(%s)->ISIN=%s"% (110, convert_symbol(source = 'security_id', dest = 'ISIN', value = 110, exchgid='SEPA'))
    print "security_id(%s)->bloomberg=%s"% (110, convert_symbol(source = 'security_id', dest = 'bloomberg', value = 110, exchgid='SEPA'))
    
    
    
    
    
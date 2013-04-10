from simep.funcs.dbtools.connections import Connections
import time # for time.clock()
from simep.funcs.data.pyData import *

bq = Connections.bases["quant"]

def get_se(mode, security_id, trading_destinations, day, context='Default'):
    """
        @param mode: estimator name; one of (volume-curve, volatility-curve, market-impact, aggressivity-levels)
        @param security_id: Security ID
        @param trading_destinations: Trading Destination - No << {MAIN} >> available for now
        @param day: Date format 'yyyymmdd'
        @param context: name of context
        @return: a list of tuples (name, value)

        See also get_se_estimator

        Examples:
            get_se ('volume-curve', 110, 4, '20100220')
            get_se ('market-impact', 110, 4, '20100220')
            get_se ('aggressivity-levels', 110, 4, '20100220')
            get_se ('volatility-curve', 110, 4, '20100220')
    """

    if mode == "volume-curve":
        ESTIMATOR_NAME = 'Volume curve'
        if context.upper() == 'DEFAULT':
            context = 'Usual day'
        estimator = get_se_estimator(ESTIMATOR_NAME, security_id, trading_destinations, day, context)
        return pyData('init', date=[x[0] for x in estimator], value={context:[x[1] for x in estimator]})

    if mode == "volatility-curve":
        ESTIMATOR_NAME = 'Volatility curve'
        if context.upper() == 'DEFAULT':
            context = 'USUAL DAY'
        estimator = get_se_estimator(ESTIMATOR_NAME, security_id, trading_destinations, day, context)
        return pyData('init', date=[x[0] for x in estimator], value={context:[x[1] for x in estimator]})

    if mode == 'market-impact':
        ESTIMATOR_NAME = 'Market Impact'
        if context.upper() == 'DEFAULT':
            context = 'Overall'
        estimator = get_se_estimator(ESTIMATOR_NAME, security_id, trading_destinations, day, context)
        return pyData('init', date=[x[0] for x in estimator], value={context:[x[1] for x in estimator]})

    if mode == 'aggressivity-levels':
        ESTIMATOR_NAME = 'Aggressivity Level'
        if context.upper() == 'DEFAULT':
            context = 'DEFAULT'
        estimator = get_se_estimator(ESTIMATOR_NAME, security_id, trading_destinations, day, context)
        return pyData('init', date=[x[0] for x in estimator], value={context:[x[1] for x in estimator]})

    print "Invalid Mode: %s" % mode

########################################################################################################

def get_se_estimator(estimator, security_id, trading_destinations, day, context):

    estimator_id = get_estimator_id(estimator)
    job_id = get_job_id(estimator_id, context, security_id, trading_destinations)

    if job_id == None:
        print "no data for (security_id: %d, trading_destination: %s)" % (security_id, trading_destinations)
        job_id = get_job_id(estimator_id, context, security_id, 'NULL')

    if job_id == None:
        print "no data for (security_id: %d, trading_destination: NULL)" % (security_id)
        job_id = get_job_id(estimator_id, context, 'NULL', trading_destinations)

    if job_id != None:
        date_se = Connections.exec_sql('QUANT',
                                       ''' SELECT  convert(varchar(8),max(er.stamp_date),112)
                                           FROM %s..estimator_runs er WHERE
                                           er.job_id = %d AND er.stamp_date <= '%s'
                                           AND estimator_id = %d
                                        ''' % (bq, job_id, day, estimator_id)
                                       )

        if date_se != None and date_se[0][0] is not None:
            data_se = Connections.exec_sql('QUANT',
                                            ''' SELECT pd.parameter_name, pv.value FROM %s..estimator_runs er,
                                            %s..param_desc pd, %s..param_value pv
                                            WHERE er.estimator_id = %d
                                            AND pv.parameter_id = pd.parameter_id
                                            AND pd.estimator_id = er.estimator_id
                                            AND pv.run_id = er.run_id
                                            AND er.run_id = (SELECT max(run_id) FROM %s..estimator_runs WHERE stamp_date='%s' AND job_id=%d and estimator_id=%d)
                                            ''' % (bq, bq, bq, estimator_id, bq, date_se[0][0], job_id, estimator_id)
                                            )
            data_se.append(('run_date', date_se[0][0]))
            return data_se
        else:
            raise ValueError("No SE entries for this date.")
    else:
        raise ValueError ("No job_id for estimator %s" % estimator)

#######################################################################################################################

def get_se_estimator_by_run_id(run_id, estimator_id, context):
    estimator = Connections.exec_sql('QUANT',
                                   ''' SELECT pd.parameter_name, pv.value
                                       FROM %s..param_desc pd, %s..param_value pv
                                       WHERE pv.parameter_id = pd.parameter_id
                                       AND pd.estimator_id = %d
                                       AND pv.run_id = %d
                                   ''' % (bq, bq, estimator_id, run_id)
                                   )

    return pyData('init', date=[x[0] for x in estimator], value={context:[x[1] for x in estimator]})

#######################################################################################################################

def get_job_id(estimator, context, s_security_id, s_trading_destination_id):

    if isinstance(estimator, int):
        estimator_id = estimator
    else:
        estimator_id = get_estimator_id(estimator)

    job_id = Connections.exec_sql("QUANT",
                                "select job_id from %s..association WHERE \
                                estimator_id=%d \
                                AND context_id=(SELECT context_id FROM %s..context WHERE context_name = \'%s\' AND estimator_id=%d)\
                                AND security_id=%s\
                                AND trading_destination_id=%s" % 
                                (bq, estimator_id, bq, context, estimator_id, s_security_id, s_trading_destination_id)
                               )
    return job_id[0][0] if len(job_id) > 0 else None

#######################################################################################################################

def get_estimator_id(estimator):
    estimator_id = Connections.exec_sql("QUANT",
                                "SELECT estimator_id FROM %s..estimator WHERE estimator_name = \'%s\'"
                                % (bq, estimator))
    return int(estimator_id[0][0]) if estimator_id is not None else None

#######################################################################################################################

def get_perimeter_sec_ids(perimeter_id):
    '''
        Returns the list of security for a perimeter
    '''

    ids = Connections.exec_sql("QUANT",
                                ''' SELECT security_id FROM %s..perimeter_domain pd
                                    JOIN %s..domain_security ds on ds.domain_id = pd.domain_id
                                    WHERE pd.perimeter_id = %d
                                ''' % (bq, bq, perimeter_id))
    return [id[0] for id in ids] if ids is not None else None

#######################################################################################################################

def get_runs_by_perimeters(perimeter_id, estimator_id, context_id):
    ids = Connections.exec_sql("QUANT",
                                ''' SELECT ds.security_id, er.stamp_date, lr.last_valid_run_id FROM %s..perimeter_domain pd
                                    JOIN %s..domain_security ds on ds.domain_id=pd.domain_id
                                    JOIN %s..association a on a.security_id=ds.security_id AND a.estimator_id= %d
                                         AND a.context_id= %d
                                         AND isnull(ds.trading_destination_id,0) = isnull(a.trading_destination_id,0)
                                    JOIN %s..last_run lr on lr.job_id=a.job_id
                                    JOIN %s..estimator_runs er on er.run_id=lr.last_valid_run_id
                                    WHERE pd.perimeter_id=%d
                                ''' % (bq, bq, bq, estimator_id, context_id, bq, bq, perimeter_id)
                            )
    return ids

#######################################################################################################################
def get_last_run_id(job_id, estimator_id):
    run_id = Connections.exec_sql('QUANT',
                          ''' SELECT last_run_id, convert(varchar(8), last_run_date, 104) last_run_date
                              FROM %s..last_run WHERE AND job_id=%d
                          ''' % (bq, estimator_id, job_id))
    return (int(run_id[0][0]), run_id[0][1]) if run_id is not None else None


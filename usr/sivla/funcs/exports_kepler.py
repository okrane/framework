from simep.funcs.dbtools.connections import Connections
import os
import csv

def export_security_indicator(perimeter, indicator_list, path, filename):

    query = "select ss.reference, td.short_name, si.indicator_id, si.indicator_value "  \
            "from quant_data..ci_security_indicator si, repository..security_source ss,  repository..security_market sm, repository..trading_destination td "  \
            "where si.security_id in (%s) and si.indicator_id in (%s) "  \
            "and ss.security_id = si.security_id and ss.trading_destination_id = sm.trading_destination_id and sm.ranking = 1 and ss.security_id = sm.security_id and ss.source_id = 2 and td.trading_destination_id = si.trading_destination_id " % ( str(perimeter).strip('[]'), str(indicator_list).strip('[]'))

    extended_query = "create view export_indicators as %s" % query

    try:
        Connections.exec_sql('BSIRIUS', "use temp_works")
        Connections.exec_sql('BSIRIUS', extended_query)

        cmd = "bcp %s out " + path + "/%s.txt -t \";\" -U batch -P delphi -S %s -c >>" + path + "/bcplog.txt"
        os.system(cmd %('temp_works..export_indicators', filename[:-4], 'BSIRIUS'   ))
        os.system(cmd %('quant_data..ci_indicator', "indicator_dictionary", 'BSIRIUS'))
    except Error as e:
        print "Error Occured:", e
    finally:
        Connections.exec_sql('BSIRIUS', "drop view export_indicators");

def export_trading_hours(path, filename):
    cmd = "bcp %s out " + path + "/%s.txt -t \";\" -U batch -P delphi -S %s -c >>" + path + "/bcplog.txt"
    os.system(cmd %('repository..trading_hours', filename[:-4], 'BSIRIUS'   ))

    query = "select top 1 * from repository..trading_hours"
    (data, schema) = Connections.exec_sql('BSIRIUS', query, True);

    writer = csv.writer(open(path + "/trading_hours_schema.txt", 'w'), delimiter = ";")
    writer.writerow(schema)

if __name__ == '__main__':
    print "Exporting Data"
    export_security_indicator([110, 2], [1, 2, 3], 'C:/KeplerExports', "security_indicator_dump.txt")
    export_trading_hours("C:/KeplerExports", "trading_hours_dump.txt");
    print "Exporting Data Ended Successfully"

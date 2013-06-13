# -*- coding: utf-8 -*-
from lib.dbtools.connections import Connections
def export_to_csv(server, query, filename, delimiter = ';'):
    result = Connections.exec_sql(server, query, as_dict = True)
    f = open(filename, "w")
    row = result[0]
    f.write( delimiter.join(row.keys()) + '\n')
    for row in result:     
        f.writelines( delimiter.join([str(x) if x else '' for x in row.values()])  + '\n')
    f.close()
    

    
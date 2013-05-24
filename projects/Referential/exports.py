# -*- coding: utf-8 -*-

from lib.dbtools.export import * 

def export_security(filename = "security.kcdb"):
    query = """select SECID, 
                SECNAME, 
                EXCHGID, 
                SYMBOL1, 
                SYMBOL2, 
                SYMBOL3, 
                SYMBOL4,
                SYMBOL5, 
                SYMBOL6,
                SYMBOL7,
                SYMBOL8,
                SYMBOL9,
                SYMBOL10,
                CCY,
                MARKET,
                SUBMARKET,
                SOURCE,
                MKTGROUP,
                PLC
                from SECURITY where STATUS='A' """   
    export_to_csv("KGR", query, filename, ";")
    

def export_security_market(filename = "security_market.kcdb"):
    query =  """select * from security_market""";   
    export_to_csv("KGR", query, filename, ";")

def export_quotation_group(filename = "quotation_group.kcdb"):
    query = """select * from QUOTATION_GROUP"""
    export_to_csv("KGR", query, filename, ";")
    
def export_trading_hours(filename = "trading_hours.kcdb"):
    query = """select * from trading_hours"""
    export_to_csv("KGR", query, filename, ";")
    

if __name__ == "__main__":
    export_security()
    export_security_market()
    export_quotation_group()
    export_trading_hours()
    


# -*- coding: utf-8 -*-

#print "python2.7 import_FIX.py HPP PARFLTLAB02 dev I CLNT1"

def upload_order_data():    
    import projects.DMAlgo.src.import_FIX as fix
    fix.export("PARFLTLAB02", "PARFLTLAB02", "dev", "I", "FLEX", ["20130531"])
    
    
upload_order_data()
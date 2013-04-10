'''
Created on 7 aout 2012

@author: gupon
'''

import executor

from XMLTestReader import ReadTestFile, PrintTestData, GetOrderList


if __name__ == '__main__':
    
    Tester = executor.AlgoTester()
    file =  "C:/st_project/PythonFix/conf/fixcfg.cfg"  
    server = executor.FIXServer(file)
    
    source = "../../config.xml"
    
    temp = ReadTestFile(source)
    
'''
Created on 18 mars 2013

@author: gpons
'''

import paramiko
import sys
import xlrd
import xml.etree.ElementTree as ET
import math

def iter_param(param_file):
    data = open(param_file,'r') 
    ListParams = {}
    
    names = None
    idx = 1
    for line in data:
        temp = line.rsplit("|")
        elt = {}
        if names == None:
            names = temp[0:len(temp)-1]
        else:
            for i in range(0,len(names)):
                elt[names[i]] = temp[i]
                
            ListParams[idx] = elt
            idx = idx + 1
    
    return ListParams

def unique(seq): 
    # order preserving
    checked = []
    for e in seq:
        if e not in checked:
            checked.append(e)
    return checked
class MatchingTA():
    
    def __init__(self):
        self.excel_file = './cfg/fix_analytics.xls'
        self.workbook = xlrd.open_workbook(self.excel_file)
    
    def getAnalytics(self, tag_nb):
        worksheet = self.workbook.sheet_by_name('Sheet1')
        num_rows = worksheet.nrows - 1
        num_cols = worksheet.ncols - 1
        
        found = 0
        curr_row = 1
        list_analytics = []
            
        while curr_row <= num_rows and found == 0:
            
            if int(worksheet.cell_value(curr_row, 2)) == int(tag_nb):
                found = 1
                curr_col = 3
                
                while curr_col <= num_cols and worksheet.cell_type(curr_row, curr_col) != 0:
                    list_analytics.append(str(worksheet.cell_value(curr_row, curr_col)))
                    curr_col += 1
                    
            curr_row += 1
            
        return list_analytics
        
class DicoFIX():
    def __init__(self):
        self.dicoFile = "./cfg/K_CSAM.xml"
        
    def getFIXtag(self, name):    
        struct = ET.parse(self.dicoFile)
        raw_data = struct.getroot()
        
        gen_conf = raw_data.find('messages')
        for elt in gen_conf.findall('message'):
            if elt.get('name') == 'NewOrderSingle':
                NOS_dico = elt
        
        for field in NOS_dico.findall('field'):
            if field.get('name') == name:
                return field.get('number')
            
        return

def getGrepFile(id_order, path, ssh):
    
    out_file = ''
    
    cmd = 'grep -r %s %s' %(id_order, path)
    
    (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
    
    out = stdout_grep.readlines()
    if len(out) > 0:
        
        line = out[0].rsplit(':')
        line = line[0].rsplit('/')
        
        out_file = line[-1]
    
    return out_file

if __name__ == '__main__':
    
    #parameters for script
    hostname = 'PARFLTLAB'
    client = 'flexapp'
    passwd = 'flexapp1'
    param_file = './inputs/basket_REtest.txt'
#     param_file = './inputs/basket_test_HUNT.txt'
#     src_warning_log = '/BOTHN31985.loG'
    day = '20130404'
    
    grep_path = '/home/flexapp/logs/trdlog/%s' %day
    
    clientFE = 'TU4'
    offset = 2
    
    mode = 'param'
    
    if mode == 'new':
        modulo = 1.0
    elif mode == 'param':
        modulo = 4.0
    elif mode == 'switch':
        modulo = 3.0
    
    #open SSH connection
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname, username=client, password=passwd)
    
    
    first_id = 'BLCL1TU4_%d' %(offset +1)
    src_warning_log = getGrepFile(first_id, grep_path, ssh)
    
    if src_warning_log == '':
        sys.exit("warning log file not found for the selected orders !")
    else:
        src_warning_log = "/%s" %src_warning_log
    
    
    #Instanciation des objets de referentiel
    dicoFix = DicoFIX()
    matcherTA = MatchingTA()
    
    linux_path_num = '/home/flexapp/logs/port/%s/%sN/%sN.consts_NUM.' %(day, clientFE, clientFE)
    linux_path_str = '/home/flexapp/logs/port/%s/%sN/%sN.consts_STR.' %(day, clientFE, clientFE)
    
    linux_path_warning = '/home/flexapp/logs/trdlog/%s' %day
    
    
    output_grep = '/warning%s.txt' %clientFE
    
    output_path = './output_analytics'
    
    ListParams = iter_param(param_file)
    
    output_file = open('%s/Analytics_%s.txt' %(output_path, day),'w')
    
    # GREP sur les warning pour le clientFE
    cmd = 'egrep \'%s.*(FIX_ORDER|CXL_RPL)\' %s%s' %(clientFE, linux_path_warning, src_warning_log)
    
    (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
    
    output_warning = open('%s%s' %(output_path, output_grep), 'w')
    for line in stdout_grep.readlines():
        output_warning.write(line)
    
    output_warning.close()
    
    id_line = 0
    
    while id_line < len(ListParams):
        
        param_line = ListParams[id_line + 1]
        
        list_analytics = []
        for u, v in param_line.items():
            if v != '':
                tag_value = dicoFix.getFIXtag(u)
                list_analytics = list(set(list_analytics) | set(matcherTA.getAnalytics(tag_value)))
        
        list_analytics = unique(list_analytics)
        
        if list_analytics != []:
            
            id_order = 'BLCL1TU4_%d' %(math.ceil((id_line + 1)/modulo) + offset)
#            id_order = 'BLCL1TU4_%d' %(id_line + offset)
            list_ana = {}
            
            #Recherche du message pour l'ordre
            output_warning = open('%s%s' %(output_path, output_grep),'r')
            for line in output_warning.readlines():
                line = line.replace('[','||')
                line = line.replace(']','||')
                
                array_line = line.rsplit('||')
                if array_line[3] == id_order:
                    print array_line[4][2:-1]
                    output_file.write("%s;COMMENT;%s;\n" %(id_order, array_line[4][2:-1]))
            
            output_warning.close()
            
            for analytic in list_analytics:
                
                print analytic
                
                sftp = ssh.open_sftp()
                t_open = 0
                try:
                    src_ANFile = sftp.open('%s%s'%(linux_path_num, analytic))
                    t_open = 1
                except IOError:
                    
                    print "NO NUMERICAL DATA FOR THIS ANALYTIC"
                    
                try:
                    src_ANFile = sftp.open('%s%s'%(linux_path_str, analytic))
                    t_open = 1
                except IOError:
                    print "NO STRING DATA FOR THIS ANALYTIC"
                
                if t_open == 1:
                    list_ana[analytic] = ""
                    for line in src_ANFile:
                        #print line
                        array_line = line.rsplit(' ')
                        if array_line[1] == id_order:
                            if '\n' in array_line[3]:
                                list_ana[analytic] = list_ana[analytic] + " || " + array_line[3][:-1]
                            else:
                                list_ana[analytic] = list_ana[analytic] + " || " + array_line[3]
                    
                    src_ANFile.close()
            
            str_output = ""
            for u, v in list_ana.items():
                str_output = "%s%s;%s;%s;\n" %(str_output, id_order, u, str(v))
            
            str_output = str_output + "\n"
            output_file.write(str_output)
            
        id_line += int(modulo)
        
    # Close File and Connection
    output_file.close()
    ssh.close()
    
    print "Export output done to : %s/Analytics_%s.txt" %(output_path, day)
    
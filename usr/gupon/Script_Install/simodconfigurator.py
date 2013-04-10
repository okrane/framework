import ConfigParser, os
from simep.funcs.dbtools.securities_by_sybase import SecuritiesBySybase
from os import path
from simep import __xml_config_file__
from simep.funcs.dbtools.securities_tools import SecuritiesTools 

class SimodConfigurator:
    def __init__(self, list_of_sec_id_to_keep, ini_file):
        self.config = ConfigParser.ConfigParser()
        found = self.config.read(ini_file)
        self.all_sections = self.config.sections()
        self.sections_to_keep = list()
      #  self.histo_dir = get_config_param(__xml_config_file__,"histo_files_dir" )
        self.sec_ids_to_keep = list_of_sec_id_to_keep
        self.sections_to_delete = list()  
    
#    def keep_selected_securities(self):
#        for s in range(len(self.all_sections)):
#           properties = self.config.options(all_sections(s))
#           sec_id = self.config.get(all_sections, "security_id")
#           if sec_id in  self.sec_ids_to_keep :
#               self.sec_ids_to_keep.append(s)
#               print "Keeping Section : %s"%s
#           else :
#               self.sections_to_delete.append(s)
#      

#    def extract_basket_info(self):
#        for s in range(len(self.all_sections)):
#            section_parts = self.all_sections[s].split('-')
#            basket_parts = self.config.get(self.all_sections[s],'basket').split('|')
#            self.isin.append(basket_parts[0])
#            self.dates.append(section_parts[0])
#            self.sec_ids.append(int(section_parts[1]))
#            self.td_ids.append(int(section_parts[2]))
#            self.open_prc.append(SecuritiesBySybase.get_trading_daily_stock_info(self.sec_ids[s],self.td_ids[s], self.dates[s])['open_prc'] )
#            self.tick_size.append(SecuritiesBySybase.get_tick_size(self.sec_ids[s],self.td_ids[s], self.open_prc[s]))
#            print "Date is : %s, sec_id is %d , ISIN : %s and td_id : %d"%(self.dates[s],self.sec_ids[s],self.isin[s],self.td_ids[s])
#    
#      
#    def generate_required_market_test_data(self):
#            for s in range(len(self.all_sections)):
#                filename = self.histo_dir + "/lobTrade_%d_%d_%s.binary\n"%(self.sec_ids[s], self.td_ids[s], self.dates[s] )
#                if not os.path.isfile(filename):
#                    ret = generateLOBFile(self.dates[s],self.sec_ids[s],self.td_ids[s],float(self.tick_size[s]),'00:00:00','19:00:00',1)
#                    print("Market data file %s not present, needs to be generated"%filename)
#                else:
#                    print("Market data file %s exist ,nothing to do"%filename)
#                    
#                
#    
#    def write_simod_config_file(self, simod_config_file):
##        [SECURITY]
##        RIC=FR0000120404
##        RFA=AC
##        securityId=2
##        tradingDestinationId=4
##        inputFileName=${HISTODIR}/lobTrade_2_4_${DATE}.binary
#        
#        with open(simod_config_file, 'w') as self.output_file:
#            for s in range(len(self.all_sections)):
#                self.output_file.write("[SECURITY]\n")
#                self.output_file.write("RIC =  %s \n"%self.isin[s])
#                # use select reference from security_source where security_id = 110     and trading_destination_id = 4 and defaut = 1 to retrive this data
#                rfa_id = SecuritiesBySybase.get_rfa_id(self.sec_ids[s],self.td_ids[s]);
#                self.output_file.write("RFA =  %s\n"%rfa_id)
#                self.output_file.write("securityId=%d\n"%self.sec_ids[s])
#                self.output_file.write("tradingDestinationId=%d\n"%self.td_ids[s])
#                self.output_file.write("inputFileName=${HISTODIR}/lobTrade_%d_%d_%s.binary\n"%(self.sec_ids[s], self.td_ids[s], self.dates[s] ))
#                self.output_file.write("\n\n");
#           
        
        
if __name__ == '__main__':
    reader = SimodConfigurator([18,26,41,110], "C:\st_sim2.5\simep\funcs\utils\data\TOPCHIX.ini")    
    reader.keep_selected_securities()

    

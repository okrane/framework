from lib.dbtools.connections import Connections
import datetime
import unittest

class TestAlgoOrders:
    def __init__(self):
        import simplejson
        file = open('test_types.json', 'r')
        input = file.read()
        file.close()
        
        # list of test that the robot should do
        self.test_list      = simplejson.loads(input)
        
        database_server     = 'TEST'
        database            = 'DB_test1'
        server_flex         = 'WATFLT01'
        environment         = 'preprod'
        source              = 'CLNT1'
        
        
        now             = datetime.datetime.now()
        delta           = datetime.timedelta(days=1)
        date            = now - delta
        dates           = [date]
#         self.data       = DatabasePlug(database_server= database_server, 
#                                        database           = database,
#                                        server_flex        = server_flex, 
#                                        environment        = environment, 
#                                        source             = source, 
#                                        dates              = dates,
#                                        mode               = "write").get_algo_orders()
#     

        client = Connections.getClient('TEST')
        db = client['DB_test1']
        orders = db['AlgoOrders']
        
        d1 = datetime.datetime(2013, 7, 2)
        d2 = datetime.datetime(2013, 7, 10)
        cursors = orders.find(
                        {"$and" :[
                                  {"SendingTime" : { "$gt" : d1} },
                                  {"SendingTime" : { "$lt" : d2} }
                                  ]
                         },
                        )
                        
        self.data = list(cursors)   
         
    def setUp(self):
        pass
    
    def test_all(self):
        for key in self.test_list.keys():
            if isinstance(self.test_list[key], basestring):
                test_result = self.run(self.test_list[key], key)
            elif isinstance(self.test_list[key], list):
                for el in self.test_list[key]:
                    test_result = self.run(el, key)
                    
    def run(self, test_name, var_name):
        method_to_call = getattr(self, test_name)
        test_result = method_to_call(var_name)
        return test_result
    
    def presence(self, var_name):
        for dic in self.data:
            assert var_name in dic.keys(), "This variable: " + var_name + " is not present in this dictionnary: " + str(dic) 
        
    def type_int(self, var_name):
        for i_dic in self.data:
            assert not var_name in i_dic.keys() or type(i_dic[var_name]) == int\
            , 'non int found for variable: ' + str(var_name) + " = " + str(i_dic[var_name]) + '\n' + str(i_dic)
        return('test passed')
    
    def type_float(self, var_name):
        for i_dic in self.data:
            assert not var_name in i_dic.keys() or type(i_dic[var_name]) == float\
            , 'non float found for variable: ' + str(var_name) + " = " + str(i_dic[var_name])+ '\n' + str(i_dic)
        return('test passed')
    
    def type_str(self, var_name):
        for i_dic in self.data:
            assert not var_name in i_dic.keys() or type(i_dic[var_name]) == str or type(i_dic[var_name]) == unicode\
            , 'non string found for variable: ' + str(var_name) + " = " + str(i_dic[var_name]) + " which has a type: " + str(type(i_dic[var_name])) + '\n' + str(i_dic)
        return('test passed')
    
    def type_num(self, var_name):
        for i_dic in self.data:
            assert not var_name in i_dic.keys() or type(i_dic[var_name]) == int or type(i_dic[var_name]) == float\
            , 'non number value found: '+ str(i_dic[var_name]) + '\n' + str(i_dic)
        return('test passed')
    def type_datetime(self, var_name):
        for i_dic in self.data:
            assert not var_name in i_dic.keys() or type(i_dic[var_name]) == datetime.datetime\
            , 'non datetime value found: '+ str(i_dic[var_name]) + '\n' + str(i_dic)
        return('test passed')
    
    def not_empty(self, var_name):
        for i_dic in self.data:
            assert var_name in i_dic.keys()\
            , 'empty value found for: ' + var_name +' \n' + str(i_dic)
        return('test passed')
    
    def positive(self, var_name):
        for i_dic in self.data:
            assert not var_name in i_dic.keys() or type(i_dic[var_name]) == int or type(i_dic[var_name]) == float\
            , 'non number value found: '+ str(i_dic[var_name]) + '\n' + str(i_dic)
            assert not var_name in i_dic.keys() or i_dic[var_name] > 0\
            , 'non positive value found for: '+ var_name + ' = ' + str(i_dic[var_name]) + ' in ' + str(i_dic) + '\n'
        return('test passed')
    
    def not_negative(self, var_name):
        for i_dic in self.data:
            assert not var_name in i_dic.keys() or type(i_dic[var_name]) == int or type(i_dic[var_name]) == float\
            , 'non number value found: '+ str(i_dic[var_name]) + '\n' + str(i_dic)
            assert not var_name in i_dic.keys() or i_dic[var_name] >= 0\
            , 'negative value found: ' + str(i_dic[var_name]) + '\n' + str(i_dic)
        return('test passed')
    
    def negative(self, var_name):
        for i_dic in self.data:
            assert not var_name in i_dic.keys() or type(i_dic[var_name]) == int or type(i_dic[var_name]) == float\
            , 'non number value found: '+ str(i_dic[var_name]) + '\n' + str(i_dic)
            assert not var_name in i_dic.keys() or i_dic[var_name] < 0\
            , 'non negative value found: ' + str(i_dic[var_name]) + '\n' + str(i_dic)
        return('test passed')
    
    def not_positive(self, var_name):
        for i_dic in self.data:
            assert not var_name in i_dic.keys() or type(i_dic[var_name]) == int or type(i_dic[var_name]) == float\
            , 'non number value found: '+ str(i_dic[var_name]) + '\n' + str(i_dic)
            assert not var_name in i_dic.keys() or i_dic[var_name] <= 0\
            , 'positive value found: ' + str(i_dic[var_name]) + '\n' + str(i_dic)
        return('test passed')        
        









robot_1 = TestAlgoOrders()
robot_1.test_all()

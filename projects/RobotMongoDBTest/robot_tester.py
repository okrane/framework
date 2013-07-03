# -*- coding: utf-8 -*-
"""
Created on Fri Jun 21 15:55:13 2013

@author: whuang
"""

class robot_tester:
    def __init__(self, name, test_list):
        # list of test that the robot should do
        self.test_list = test_list
        self.name = name
    def work(self, data):
        # robot will start its test
        print 'start tests:'
        print '-----------------------------------------------------------'
        for test in self.test_list:
            print test + ': '      
            test_result = self.test(data, test)
            print test_result
            print '-----------------------------------------------------------'
    def test(self, data, test):
        test_interpret = test.split()
        var_name = test_interpret[0]
        test_name = test_interpret[1]
        methodtocall = getattr(self, test_name)
        if len(test_interpret) > 2:
            test_para = test_interpret[2]
            test_result = methodtocall(data, var_name, test_para)
        else:
            test_result = methodtocall(data, var_name)
        return test_result
    def test_typeint(self, data, var_name):
        for i_dic in data:
            assert not var_name in i_dic.keys() or type(i_dic[var_name]) == int\
            , 'non int found! ' + str(i_dic[var_name]) + '\n' + str(i_dic)
        return('test passed')
    def test_typefloat(self, data, var_name):
        for i_dic in data:
            assert not var_name in i_dic.keys() or type(i_dic[var_name]) == float\
            , 'non float found! ' + str(i_dic[var_name]) + '\n' + str(i_dic)
        return('test passed')
    def test_typestr(self, data, var_name):
        for i_dic in data:
            assert not var_name in i_dic.keys() or type(i_dic[var_name]) == str or type(i_dic[var_name]) == unicode\
            , 'non string found! ' + str(i_dic[var_name]) + '\n' + str(i_dic)
        return('test passed')
    def test_typenum(self, data, var_name):
        for i_dic in data:
            assert not var_name in i_dic.keys() or type(i_dic[var_name]) == int or type(i_dic[var_name]) == float\
            , 'non number value found: '+ str(i_dic[var_name]) + '\n' + str(i_dic)
        return('test passed')
    def test_notempty(self, data, var_name):
        for i_dic in data:
            assert var_name in i_dic.keys()\
            , 'empty value found: \n' + str(i_dic)
        return('test passed')
    def test_positive(self, data, var_name):
        for i_dic in data:
            assert not var_name in i_dic.keys() or type(i_dic[var_name]) == int or type(i_dic[var_name]) == float\
            , 'non number value found: '+ str(i_dic[var_name]) + '\n' + str(i_dic)
            assert not var_name in i_dic.keys() or i_dic[var_name] > 0\
            , 'non positive value found: ' + str(i_dic[var_name]) + '\n' + str(i_dic)
        return('test passed')
    def test_nonnegative(self, data, var_name):
        for i_dic in data:
            assert not var_name in i_dic.keys() or type(i_dic[var_name]) == int or type(i_dic[var_name]) == float\
            , 'non number value found: '+ str(i_dic[var_name]) + '\n' + str(i_dic)
            assert not var_name in i_dic.keys() or i_dic[var_name] >= 0\
            , 'negative value found: ' + str(i_dic[var_name]) + '\n' + str(i_dic)
        return('test passed')
    def test_negative(self, data, var_name):
        for i_dic in data:
            assert not var_name in i_dic.keys() or type(i_dic[var_name]) == int or type(i_dic[var_name]) == float\
            , 'non number value found: '+ str(i_dic[var_name]) + '\n' + str(i_dic)
            assert not var_name in i_dic.keys() or i_dic[var_name] < 0\
            , 'non negative value found: ' + str(i_dic[var_name]) + '\n' + str(i_dic)
        return('test passed')
    def test_nonpositive(self, data, var_name):
        for i_dic in data:
            assert not var_name in i_dic.keys() or type(i_dic[var_name]) == int or type(i_dic[var_name]) == float\
            , 'non number value found: '+ str(i_dic[var_name]) + '\n' + str(i_dic)
            assert not var_name in i_dic.keys() or i_dic[var_name] <= 0\
            , 'positive value found: ' + str(i_dic[var_name]) + '\n' + str(i_dic)
        return('test passed')        
        
        
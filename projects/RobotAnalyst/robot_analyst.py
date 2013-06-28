# -*- coding: utf-8 -*-
"""
Created on Fri Jun 21 15:55:13 2013

@author: whuang
"""
import numpy
import scipy.stats.mstats as stats
import matplotlib.pyplot as plt
import math

class robot_analyst:
    def __init__(self, name, kbase, target):
        # construct the knowledge base (a list) of the robot analyst
        self.kbase = kbase
        # the robot's name (a string) suggests his speciality
        self.name = name
        # the work list (a dictionary) that the robot will try to accomplish
        self.target = target
        # robot analyst will express his conclusions in this list
        self.conclusion = []
    def work(self, data):
        # robot will use his knowledge base to do his works
        for i_target in self.target.keys():
            self.solve(data, i_target, self.target[i_target])
        for i in range(len(self.conclusion)):
            print self.conclusion[i]
    def think(self, data, targetname, targetpara):
        # robot will search the data, try to correct/enrich his knowledge base
        self.search(data, targetname, targetpara)
    def solve(self, data, targetname, targetpara):
        # robot will try to solve the target question with the intened parameters
        return        
    def search(self, data, targetname, targetpara):
        return
    def answer(self, data, question):
        return

class robot_analyst_statistics(robot_analyst):
    # a robot specialised at statistical analysis
    def __init__(self, name, kbase, target):
        robot_analyst.__init__(self, name, kbase, target)
    def solve(self, data, targetname, targetpara):
        if targetname == 'Test_Uniform_Discrete':
            # example test to see if aggregated by some discrete value, the number of sequence follows an uniform distritbution
            nb_study = len(targetpara)
            for i_study in range(nb_study):
                var_study = data[targetpara[i_study]]
                value_list = numpy.unique(var_study)
                value_list.sort()
                var_nb = numpy.empty(len(value_list))
                for i_value in range(len(value_list)):
                    var_nb[i_value] = sum(var_study == value_list[i_value])
                test_chi2 = stats.chisquare(var_nb)
                if test_chi2[1] > 0.005:
                    self.conclusion.append(targetpara[i_study] + ' follows approximately a discrete uniform distribution on: \n       '
                    + str(value_list.values) + '.')
                else:
                    self.conclusion.append(targetpara[i_study] + ' does not follow a discrete uniform distribution on: \n       '
                    + str(value_list.values) + '.')
        return
    def answer(self, data, question):
        structured_question = question.split()
        question_type = structured_question[0]
        agg_method = structured_question[1]
        if 'of' in question:
            column = structured_question[3]
            aggregator = structured_question[5]
            value_at_question = structured_question[6]
            assertion = structured_question[8]
        else:
            column = ''
            aggregator = structured_question[3]
            value_at_question = structured_question[4]
            assertion = structured_question[6]
        if question_type == 'why' or question_type == 'Why':
            reply = self.answer_why(data, agg_method, column, aggregator, value_at_question, assertion)
        elif question_type == 'whether' or question_type == 'Whether':
            reply = self.answer_whether(data, agg_method, column, aggregator, value_at_question, assertion)
        return reply
    def answer_whether(self, data, agg_method, column, aggregator, value_at_question, assertion):
        aggregator_value = data[aggregator].values
        agg_list = numpy.unique(aggregator_value)
        agg_list.sort()
        var_agg = numpy.empty(len(agg_list))
        nb_observation = len(aggregator_value)
        if agg_method == 'avg' or agg_method == 'avg_turnover':
            var_column = data[column]
        if agg_method == 'avg_turnover' or agg_method == 'turnover':
            turnover_euro = data['turnover_euro']
        for i_value in range(len(agg_list)):
            if str(agg_list[i_value]) == value_at_question:
                id_at_question = i_value
            if agg_method == 'number':
                var_agg[i_value] = sum(aggregator_value == agg_list[i_value])
            elif agg_method == 'turnover':
                var_agg[i_value] = sum(turnover_euro[aggregator_value == agg_list[i_value]])
            elif agg_method == 'avg':
                var_agg[i_value] = numpy.mean(var_column[aggregator_value == agg_list[i_value]])
            elif agg_method == 'avg_turnover':
                var_agg[i_value] = numpy.average(var_column[aggregator_value == agg_list[i_value]], turnover_euro[aggregator == agg_list[i_value]])
        if assertion == 'big' and (agg_method == 'turnover' or agg_method == 'number'): 
            reply = self.test_big_distribution(var_agg, id_at_question, nb_observation)
        elif assertion == 'small' and (agg_method == 'turnover' or agg_method == 'number'): 
            reply = self.test_small_distribution(var_agg, id_at_question, nb_observation)
        elif assertion == 'big' and (agg_method == 'avg' or agg_method == 'avg_turnover'): 
            reply = self.test_big_value(var_agg, id_at_question, nb_observation)
        elif assertion == 'small' and (agg_method == 'avg' or agg_method == 'avg_turnover'): 
            reply = self.test_small_value(var_agg, id_at_question, nb_observation)
        print reply['comment']
        return reply
    def answer_why(self, data, agg_method, column, aggregator, value_at_question, assertion):  
        knowledge_key_det = 'det+' + agg_method + '+' + column + '+' + aggregator
        knowledge_key_rand = 'rand' + agg_method + '+' + column + '+' + aggregator
        reply = {}
        if not knowledge_key_det in self.kbase and not knowledge_key_rand in self.kbase:
            print 'I do not have the necessary knowledge to answer this question. '
            return reply
        if knowledge_key_det in self.kbase:
            knowledge_det = self.kbase[knowledge_key_det]
            related_var_list = knowledge_det.split('+')
            for i_var in range(len(related_var_list)):
                var_related = related_var_list[i_var]
                reply[related_var_list[i_var]] = self.why_deterministic(data, aggregator, agg_method, column, value_at_question, assertion, var_related)
        return reply
    def test_big_distribution(self, var_agg, id_at_question, nb_observation):
        prob_id_at_question = var_agg[id_at_question] / sum(var_agg)
        delta_value = prob_id_at_question*(1-prob_id_at_question)/math.sqrt(nb_observation)*3   
        reply = {}        
        if prob_id_at_question - delta_value > 1.5/len(var_agg):
            reply['answer'] = True
            reply['comment'] = 'Yes, it is significantly bigger than an uniformly distributed random variable. '
            return reply
        else:
            reply['answer'] = False
            reply['comment'] = 'No, it is not significantly bigger than an uniformly distributed random variable. '
            return reply
    def test_small_distribution(self, var_agg, id_at_question, nb_observation):
        prob_id_at_question = var_agg[id_at_question] / sum(var_agg)
        delta_value = prob_id_at_question*(1-prob_id_at_question)/math.sqrt(nb_observation)*3    
        reply = {}                
        if prob_id_at_question + delta_value < 0.5/len(var_agg):
            reply['answer'] = True
            reply['comment'] = 'Yes, it is significantly smaller than an uniformly distributed random variable. '
            return reply
        else:
            reply['answer'] = False
            reply['comment'] = 'No, it is not significantly smaller than an uniformly distributed random variable. '
            return reply
    def test_big_value(self, var_agg, id_at_question, nb_observation):
        return False
    def test_small_value(self, var_agg, id_at_question, nb_observation):
        return False
    def why_deterministic(self, data, aggregator, agg_method, column, value_at_question, assertion, var_related):
        aggregator_value = data[aggregator].values
        var_related_value = data[var_related].values
        var_related_list = numpy.unique(var_related_value)
        agg_list = numpy.unique(aggregator_value)
        agg_list.sort()
        var_agg_filtered = numpy.empty(len(agg_list))
        var_agg_filtered_without = numpy.empty(len(agg_list))
        reply = {}
        reply['main_reasons'] = ''
        reply['reasons'] = ''
        for i_value_agg in range(len(agg_list)):
            if str(agg_list[i_value_agg]) == value_at_question:
                id_at_question = i_value_agg
        if agg_method == 'number' and assertion == 'big':
            for i_value_related in range(len(var_related_list)):
                aggregator_value_filtered = aggregator_value[var_related_value == var_related_list[i_value_related]]
                aggregator_value_filtered_without = aggregator_value[var_related_value != var_related_list[i_value_related]]
                nb_observation_filtered = len(aggregator_value_filtered)
                nb_observation_filtered_without = len(aggregator_value_filtered_without)
                for i_value_agg in range(len(agg_list)):               
                    var_agg_filtered[i_value_agg] = sum(aggregator_value_filtered == agg_list[i_value_agg])
                    var_agg_filtered_without[i_value_agg] = sum(aggregator_value_filtered_without == agg_list[i_value_agg])
                reply_filter_tmp = self.test_big_distribution(var_agg_filtered, id_at_question, 
                                                              nb_observation_filtered)
                reply_filter_without_tmp = self.test_big_distribution(var_agg_filtered_without, 
                                                                      id_at_question, nb_observation_filtered_without)
                if reply_filter_tmp['answer'] and not reply_filter_without_tmp['answer']:
                    if reply['main_reasons'] == '':
                        reply['main_reasons'] = str(var_related_list[i_value_related])
                    else:
                        reply['main_reasons'] = reply['main_reasons'] + '+' + str(var_related_list[i_value_related])
                elif reply_filter_tmp['answer']:
                    if reply['reasons'] == '':
                        reply['reasons'] = str(var_related_list[i_value_related])
                    else:
                        reply['reasons'] = reply['reasons'] + '+' + str(var_related_list[i_value_related])
            main_reasons = reply['main_reasons'].split('+')
            reasons = reply['reasons'].split('+')
            if main_reasons[0] == '' and reasons[0] == '':
                print 'Variable: ' + str(var_related) + ' is not related to this question. ' 
                return {}
            if main_reasons[0]!= '': 
                print '----------------------- Mains Reasons, by ' + str(var_related) + ': --------------------------'
                for i_main_reasons in range(len(main_reasons)):
                    print_str =  'A ' + assertion + ' numbers of orders of ' + str(var_related) + ' ' + main_reasons[i_main_reasons] + ' are sent at ' + aggregator + ' ' + str(value_at_question) + ', '
                    print_str = print_str +  'if we do not include these ordres, the number of orders sent at ' + aggregator + ' ' + str(value_at_question) + ' becomes not sigifincantly ' + assertion + '. '
                    print print_str
                self.bar_discrete_number(data, aggregator, var_related)
            if reasons[0]!= '':
                print '----------------------- Possible Explanations, by ' + str(var_related) + ': --------------------------'
                for i_reasons in range(len(reasons)):
                    print 'A ' + assertion + ' numbers of orders of ' + str(var_related) + ' ' + reasons[i_reasons] + ' are sent at ' + aggregator + ' ' + str(value_at_question) 
            return reply
        elif agg_method == 'number' and assertion == 'small':
            for i_value_related in range(len(var_related_list)):
                aggregator_value_filtered = aggregator_value[var_related_value == var_related_list[i_value_related]]
                aggregator_value_filtered_without = aggregator_value[var_related_value != var_related_list[i_value_related]]
                nb_observation_filtered = len(aggregator_value_filtered)
                nb_observation_filtered_without = len(aggregator_value_filtered_without)
                for i_value_agg in range(len(agg_list)):               
                    var_agg_filtered[i_value_agg] = sum(aggregator_value_filtered == agg_list[i_value_agg])
                    var_agg_filtered_without[i_value_agg] = sum(aggregator_value_filtered_without == agg_list[i_value_agg])
                reply_filter_tmp = self.test_small_distribution(var_agg_filtered, id_at_question, 
                                                              nb_observation_filtered)
                reply_filter_without_tmp = self.test_small_distribution(var_agg_filtered_without, 
                                                                      id_at_question, nb_observation_filtered_without)
                if reply_filter_tmp['answer'] and not reply_filter_without_tmp['answer']:
                    if reply['main_reasons'] == '':
                        reply['main_reasons'] = str(var_related_list[i_value_related])
                    else:
                        reply['main_reasons'] = reply['main_reasons'] + '+' + str(var_related_list[i_value_related])
                elif reply_filter_tmp['answer']:
                    if reply['reasons'] == '':
                        reply['reasons'] = str(var_related_list[i_value_related])
                    else:
                        reply['reasons'] = reply['reasons'] + '+' + str(var_related_list[i_value_related])
            main_reasons = reply['main_reasons'].split('+')
            reasons = reply['reasons'].split('+')
            if main_reasons[0] == '' and reasons[0] == '':
                print 'Variable: ' + str(var_related) + ' is not related to this question. ' 
                return {}
            if main_reasons[0]!= '': 
                print '----------------------- Main Explanations, by ' + str(var_related) + ': --------------------------'
                for i_main_reasons in range(len(main_reasons)):
                    print_str =  'A ' + assertion + ' numbers of orders of ' + str(var_related) + ' ' + main_reasons[i_main_reasons] + ' are sent at ' + aggregator + ' ' + str(value_at_question) + ', '
                    print_str = print_str +  'if we do not include these ordres, the number of orders sent at ' + aggregator + ' ' + str(value_at_question) + ' becomes not sigifincantly ' + assertion + '. '
                    print print_str
                self.bar_discrete_number(data, aggregator, var_related)
            if reasons[0]!= '':
                print '----------------------- Possible Explanations, by ' + str(var_related) + ': --------------------------'
                for i_reasons in range(len(reasons)):
                    print 'A ' + assertion + ' numbers of orders of ' + str(var_related) + ' ' + reasons[i_reasons] + ' are sent at ' + aggregator + ' ' + str(value_at_question)
            return reply
        return reply
    def bar_discrete_number(self, data, principal_var, secondary_var):
        var_principal = data[principal_var].values
        var_secondary = data[secondary_var].values
        list_principal = numpy.unique(var_principal)
        list_secondary = numpy.unique(var_secondary)
        list_principal.sort()
        var_agg_principal = numpy.empty(len(list_principal))
        p = []
        ind = numpy.arange(len(list_principal))
        nb_total = len(var_principal)
        width = 0.4
        i_color = 0
        legend_add = []
        plt.figure(None, figsize=(20,8))
        for i_value_secondary in list_secondary:
            var_principal_filtered = var_principal[var_secondary == i_value_secondary]
            for i_principal in range(len(list_principal)):
                var_agg_principal[i_principal] = sum(var_principal_filtered == list_principal[i_principal])
            if len(p) == 0:
                p = plt.bar(ind, 1.0*var_agg_principal/nb_total, width, color = numpy.random.random(3))
                i_color = i_color + 1
                cum_var_agg_principal = 1.0*var_agg_principal/nb_total
            else:
                p = plt.bar(ind, 1.0*var_agg_principal/nb_total, width, color = numpy.random.random(3),
                                 bottom = cum_var_agg_principal)
                cum_var_agg_principal = cum_var_agg_principal + 1.0*var_agg_principal/nb_total
                i_color = i_color + 1
            legend_add.append(str(secondary_var) + '==' + str(i_value_secondary))
        plt.legend(tuple(legend_add))
        plt.ylabel('Percentage by Numbers')
        plt.xlabel(str(principal_var))
        plt.title('Discrete Distribution Plot by ' + str(principal_var) + ' and ' + str(secondary_var))
        plt.xticks(ind + width/2.0, list_principal)
        plt.yticks(numpy.arange(0,max(cum_var_agg_principal),0.05))
        plt.show()
        
                                    
                
                
                
                
                
                
                
                
                
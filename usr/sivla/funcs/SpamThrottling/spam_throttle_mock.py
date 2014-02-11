# -*- coding: utf-8 -*-

class Store:
    def __init__(self):
        self.last_time = 0
        self.count = 0

def algorithm(target_interval, max_count, arrival_arrays):
    store = Store()
    for t in arrival_arrays:
        print t, store.count, store.last_time
        if store.last_time == 0:
            store.last_time = t
            store.count = 1
            continue
        #print 1.0 * (store.count)/(t - store.last_time), 1.0 * max_count / target_interval
        if 1.0 * (store.count)/(t - store.last_time) > 1.0 * max_count / target_interval:
            #print "sending ", t
            store.count += 1
        else:
            store.last_time = t
            store.count = 1
            
        if store.count > max_count:
            print "error ", t, " will be blocked"
            store.count -= 1
            
    
target_interval = 10
max_count = 2
#sample_times =[1, 2, 3, 4, 5, 11, 34, 55, 89, 111, 112, 115, 117, 119, 120]
sample_times =[1, 7, 8, 9, 10 ,11, 12, 13, 14, 15]

#sample_times = range(1, 30)

algorithm(target_interval, max_count, sample_times)

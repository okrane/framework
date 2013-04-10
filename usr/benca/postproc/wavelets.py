'''
Created on 24 janv. 2011

@author: benca
'''

import numpy as n
import matplotlib
import matplotlib.pyplot as p
import pywt as w

def load_data(filename, i):
    from datetime import datetime
    f = open(filename, 'r')
    L = f.readlines()
    f.close()
    data = []
    t    = []
    for l in L:
        fields = tuple(l.split('|'))
        t.append(datetime.strptime(fields[0], '%H:%M:%S'))
        data.append(float(fields[i]))
    if len(t)%2 != 0:
        t.pop(-1)
        data.pop(-1)
    bm = sum(data)/len(data)
    for i in range(len(data)):
        data[i] -= bm
    return (t, data)

def show_figure():
    p.figure(1)
    px = p.subplot(111)
    px.xaxis.set_major_formatter( (matplotlib.dates.DateFormatter('%H:%M')))
    for label in px.xaxis.get_ticklabels():
        label.set_rotation(-30)
    matplotlib.pyplot.xlabel('time')
    matplotlib.pyplot.ylabel('price')
    p.grid(True)
    p.figure(2)
    matplotlib.pyplot.xlabel('time')
    matplotlib.pyplot.ylabel('price')
    p.grid(True)
    p.show()

def filter_data(D, thresholds, wavelets):
    colors = ['g', 'c', 'b', 'y', 'k', 'r', 'm']
    wavelets_perf = dict([(wt, []) for wt in wavelets])
    for threshold in thresholds:
        best_error = 100000000000000000.0
        best_wavelet = ''
        for (id,wavelet) in enumerate(wavelets):
            print '######################################################'
            print w.Wavelet(wavelet)
            print '------------------------------------------------------'
            C = w.wavedec(D[1], wavelet, 'sp1')
            reshaped_length = sum([len(c) for c in C])
            tmp = n.array([0.0 for i in range(reshaped_length)])
            a = 0
            for v in C:
                tmp[a:a+len(v)] = v
                a += len(v)
            full_vector = tmp
            tmp = n.abs(tmp)
            tmp[::-1].sort()
            print tmp[:20]
            index_thr = int(threshold*len(tmp))
            thr = tmp[index_thr]
            print 'thr_idx = %d, thr = %f' %(index_thr, thr)
            print '------------------------------------------------------'
            for i in range(len(C)):
                C[i] = w.thresholding.hard(C[i], thr)
            R  = w.waverec(C, wavelet, 'sp1')
            p.plot_date(D[0],  R, xdate=True, ydate=False, color=colors[id%len(colors)], linestyle='-', linewidth=1, marker='None')
            err = n.sum(n.abs(R-n.array(D[1])))
            print 'L1 error = %f' %err
            print '------------------------------------------------------'
            wavelets_perf[wavelet].append(err)
            if err < best_error:
                best_error = err
                best_wavelet = wavelet
        print '######################################################'
        print 'Best results : %s with %f error' %(best_wavelet, best_error)
        print '######################################################'
    p.figure(2)
    i=0
    for (k,v) in wavelets_perf.iteritems():
        print k, ' in ', colors[i%len(colors)]
        p.plot(thresholds, v, color=colors[i%len(colors)], linestyle='-', linewidth=1, marker='None')
        i+=1
#    for (i,c) in enumerate(C):
#        p.plot(range(len(c)), c, color=colors[i%len(colors)], linestyle='-', linewidth=1, marker='None')
    return R
    


if __name__ == '__main__':
    
    '''######################################################################################################
    #############################################   PARAMETERS   ############################################
    ######################################################################################################'''
    
    stock    = 'AIRP.PA'
    filename = 'C:/uniform_curves.txt_' + stock + '_20110118.txt'
    #wavelets = ['coif1', 'coif2', 'coif3', 'coif4', 'coif5']
    #wavelets = ['rbio2.4', 'rbio4.4', 'rbio5.5']
    #wavelets = ['sym2', 'sym3', 'sym4', 'sym5', 'sym6', 'sym7', 'sym8', 'sym9', 'sym10', 'sym11', 'sym12', 'sym13', 'sym14', 'sym15', 'sym16', 'sym17', 'sym18', 'sym19', 'sym20']
    wavelets = ['db3', 'bior2.4', 'bior4.4', 'sym6']
    wavelets = ['db3']
    #thresholds = [0.02, 0.0225, 0.025, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10]
    thresholds = [0.1]
    index     = 1
    ''' 
    some results (30s sampling) with :
    -> db3 got best results among daubechies for any reasonable threshold
    -> bior2.4 and bior4.4 got best results among daubechies
    -> coif2 got best results but poor compared to the others
    -> rbio4.4, rbio5.5 got the best results but poor compared to the others
    -> sym3, sym6
    '''
    
    
    '''######################################################################################################
    ################################################   CODE   ###############################################
    ######################################################################################################'''
    
    D = load_data(filename, index)
    print 'Number of points = %d' %len(D[0])
    p.figure(1)
    p.plot_date(D[0],  D[1], xdate=True, ydate=False, color='r', linestyle='-', linewidth=1, marker='None')
    R = filter_data(D, thresholds, wavelets)
    show_figure()
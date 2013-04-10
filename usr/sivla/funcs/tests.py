#strategy = '1'
#client = '1'
#server = '1'

#import numpy as np 
#import matplotlib.pyplot as plt 
#x = np.arange(10) 
#plt.step(x,x) 
#plt.show()

from simep.funcs.data.pyData import pyData

x = pyData('csv', filename = 'C:/KeplerExports/security_indicator_dump.txt', fieldnames = ['ric', 'dest', 'id', 'value'], delimiter = ';')
print x
#print x.value

print x.interval(0, 2)
print x['ric'][x.date.index(20)]
x.plot()

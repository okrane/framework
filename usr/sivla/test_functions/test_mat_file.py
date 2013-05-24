# -*- coding: utf-8 -*-

import scipy.io
mat = scipy.io.loadmat('Q:/dev_repository/get_tick/ft/FTE/2013_05_01.mat', struct_as_record  = False)
print mat['data'][0][0].value

#values = mat['data'][1]
#colnames = mat['data'][2]
#print title


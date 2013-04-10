from simep.utils import *

date_start = 20101101
sec_ids = [2,
           8,
          11,
          18,
          26,
          31,
          41,
          45,
          71,
          92,
         110,
         157,
         160,
         174,
         187,
         192,
         208,
         209,
         210,
         216,
         220,
         232,
         238,
         246,
         257,
         273,
         276,
         280,
         286,
       13280,
       16827,
       43509,
       43542,
       58567,
      107509,
      290428,
      294403,
      312417,
      673005]

trading_destination_id = 4
tickSize = 0.005
beginTime = '09:00:00'
endTime   = '17:30:00'

for i in range(1, 30):
    for j in range(len(sec_ids)):
        try:       
            generateLOBFile(str(date_start + i), sec_ids[j], trading_destination_id, tickSize, beginTime, endTime, 1)
        except:
            print 'Error on binarizing file', date_start + i, sec_ids[j], trading_destination_id


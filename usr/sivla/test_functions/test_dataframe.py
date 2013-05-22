from lib.data.st_data import * 
 
#data = from_mat_file("Q:/dev_repository/get_tick/ft/FTE/2013_05_01.mat")
"""
grouped=data.groupby(pd.TimeGrouper(freq='5Min')) 
for k, v in grouped:
    print v["bid"]
    """
data=pd.DataFrame({'A' : np.array([1,2,1,3,2,3,2]), 'B' :np.array([2,2,2,3,2,3,2])})
a=data[['A','B']].values
a.view([('', a.dtype)]*a.shape[1])

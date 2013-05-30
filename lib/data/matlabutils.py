# -*- coding: utf-8 -*-
"""
Created on Tue May 21 15:36:38 2013

@author: njoseph
"""

import numpy as np


###############################################################################
# uniqueext
###############################################################################

def uniqueext(a,return_index=False,return_inverse=False,rows=False):
    #--------------------------------------------------------------------------
    # CASE 1 : numpy ndarray
    #--------------------------------------------------------------------------
    if (isinstance(a,np.ndarray)) and (not rows):
        ### Check
        if (len(a.shape)>1):
            if (a.shape[0]>1) and (a.shape[1]>1):
                raise NameError('uniqueext:check - rows is at false, this is what you want ??')
        
        ### Compute : and keep the input structure shape !
        if len(a.shape)<=1:
            return np.unique(a,return_index=return_index,return_inverse=return_inverse)
        else:
            res=np.unique(a,return_index=return_index,return_inverse=return_inverse)
            if (not return_index) & (not return_inverse):
                return res.reshape(res.size,1)
            elif (len(res)==1):
                return res[0].reshape(res[0].size,1),res[1]
            else:
                return res[0].reshape(res[0].size,1),res[1],res[2]
    #--------------------------------------------------------------------------
    # CASE 2 : numpy ndarray rows
    #--------------------------------------------------------------------------    
    elif (isinstance(a,np.ndarray)) & rows:
        if len(a.shape)<=1: # this is one row
            if return_index & return_inverse:
                return a,np.array([0]),np.array([0])
            elif return_index | return_inverse:
                return a,np.array([0])
            else:
                return a
        else: # this is a matrix or a column
            # aintuple=a.view([('', a.dtype)]*a.shape[1])
            aintuple=[tuple(x) for x in a]
            #unique_aintuple = np.unique(aintuple)
            unique_aintuple=list(set(aintuple))
            #unique_a=unique_aintuple.view(a.dtype).reshape((unique_aintuple.shape[0], a.shape[1]))
            unique_a=np.array(unique_aintuple)
            if (not return_index) & (not return_inverse):
                return unique_a
            else:
                ## compute
                return_inverse_vals=[];
                return_index_vals=[];
                if return_inverse:
                    for v in aintuple:
                        return_inverse_vals.append([idx for (idx,val) in enumerate(unique_aintuple) if val==v][0])
                if return_index:
                    for v in unique_aintuple:
                        return_index_vals.append(max([idx for (idx,val) in enumerate(aintuple) if val==v]))
                ## output   
                if return_index & return_inverse:
                    return unique_a,return_index_vals,return_inverse_vals
                elif return_index:
                    return unique_a,return_index_vals
                elif return_inverse:
                    return unique_a,return_inverse_vals
    #--------------------------------------------------------------------------
    # CASE 3 : list
    #--------------------------------------------------------------------------
    elif (isinstance(a,list)) and (not rows):
        return uniqueext(np.array(a),return_index=return_index,return_inverse=return_inverse,rows=rows)
    else:
        raise NameError('uniqueext:input - bad input type')
  

      
###############################################################################
# ismember
###############################################################################
        
def ismember(a,b,rows=False):
    #--------------------------------------------------------------------------
    # CASE 1 : numpy ndarray
    #--------------------------------------------------------------------------
    if isinstance(a,np.ndarray) and isinstance(b,np.ndarray) and (not rows):
        ### Check a and b
        if (((len(a.shape)>1) and (a.shape[0]>1) and (a.shape[1]>1)) or 
            (len(b.shape)>1) and (b.shape[0]>1) and (b.shape[1]>1)):
                raise NameError('ismember:check - rows is at false, this is what you want ??')                
        tf = np.array([i in b for i in a])
        index=np.array([-1]*len(tf) ) # TRES SALE le -1, ia pas de NaN en array
        # index = np.array([(np.where(b == i))[0][-1] if t else 0 for i,t in zip(a,tf)])
        if not any(tf):
            return tf,index  
        idx_ina=tf.nonzero()[0]
        uni_ainb,idx_in_uni_ainb = np.unique(a[tf],return_inverse=True) 
        index_uni = np.array([(np.where(b == i))[0][-1] for i in uni_ainb])
        index[idx_ina]=index_uni[idx_in_uni_ainb]
        return tf, index
    else:
        raise NameError('ismember:input - bad input type')
        
        
###############################################################################
# isint
###############################################################################
def isint(a):
    try:
        int(a)
        return True
    except:
        return False
    
    
    
    
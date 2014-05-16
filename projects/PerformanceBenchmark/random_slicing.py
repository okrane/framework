# -*- coding: utf-8 -*-
"""
Created on Mon Jan 27 10:32:23 2014

@author: whuang
"""

import random
import numpy as np

# for two given enveloppe, return a random trajectory within these enveloppes.
def random_slicing(env_max,env_min,method,theta=1,k=1):
    nb_slice = len(env_max)
    trajectory = np.zeros(nb_slice)
    if method == 'uniform':
        for i in range(nb_slice-1):
            ratio = random.random()
            max_target_volume = env_max[i+1] - trajectory[i]
            min_target_volume = max(0,env_min[i+1] - trajectory[i])
            volume = min_target_volume + (max_target_volume - min_target_volume)*ratio
            trajectory[i+1] = trajectory[i] + volume
    elif method == 'center':
        for i in range(nb_slice-1):
            ratio = random.random()
            target_volume = env_min[i+1] + (env_max[i+1] - env_min[i+1])*ratio
            trajectory[i+1] = max(trajectory[i],target_volume)
    elif method == 'gamma_centered':
        # follow the linear tracking trajectory
        k = env_max[-1]/(nb_slice-1)/theta
        for i in range(nb_slice-1):
            exec_volume_theo = np.random.gamma(k,theta)
            max_target_volume = env_max[i+1] - trajectory[i]
            min_target_volume = max(0,env_min[i+1] - trajectory[i])
            exec_volume = min(max_target_volume,max(exec_volume_theo,min_target_volume))
            trajectory[i+1] = trajectory[i] + exec_volume
    elif method == 'gamma_centered_2':
        # follow the mid trajectory
        for i in range(nb_slice-1):
            k = 0.5*(env_max[i+1]-env_max[i]+env_min[i+1]-env_min[i])/theta
            exec_volume_theo = np.random.gamma(k,theta)
            max_target_volume = env_max[i+1] - trajectory[i]
            min_target_volume = max(0,env_min[i+1] - trajectory[i])
            exec_volume = min(max_target_volume,max(exec_volume_theo,min_target_volume))
            trajectory[i+1] = trajectory[i] + exec_volume
    elif method == 'uniform_2':
        # follow the mid trajectory, by uniform dsitribution
        for i in range(nb_slice-1):
            k = env_max[i+1]-env_max[i]+env_min[i+1]-env_min[i]
            exec_volume_theo = random.random()*k
            max_target_volume = env_max[i+1] - trajectory[i]
            min_target_volume = max(0,env_min[i+1] - trajectory[i])
            exec_volume = min(max_target_volume,max(exec_volume_theo,min_target_volume))
            trajectory[i+1] = trajectory[i] + exec_volume
    
    return trajectory
            
    
    





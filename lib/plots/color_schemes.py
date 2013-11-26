# -*- coding: utf-8 -*-

import numpy as np

def kc_main_colors(one_scale = True):
    """ Returns the 6 main colors for Kepler Chevreux in RGB
    @param one_scale: if True the result will be on a scale of 0 to 1, if False the scale will be 0 to 256
    """       
    colors = {"light_blue": np.array([178, 219, 238]), 
            "blue_1" : np.array([128, 195, 227]), 
            "blue_2": np.array([51, 158, 210]), 
            "dark_blue" : np.array([0, 51, 102]),
            "light_violet": np.array([178, 194, 209]), 
            "violet": np.array([128, 153, 179]), 
            "light_orange": np.array([248, 202, 158]), 
            "orange": np.array([240, 149, 61])}  
    if one_scale:
        return dict((k, 1.0 * v / 256) for k, v in colors.iteritems())
    return colors

def nice_colors(one_scale = True):
    colors = {"dark_red" : np.array([196, 30, 58])}
    if one_scale:
        return dict((k, 1.0 * v / 256) for k, v in colors.iteritems())
    return colors
    
def color_grayscale(base_color, nb_points = 5):
    """ Returns a grayscale starting from the base color
    @param base_color: the starting rgb color for the grayscale
    @param nb_points: the number of points in the grayscale
    """           
    base_color = np.array(base_color)    
    result = []
    for i in range(1, nb_points):
        result.append(1.0 * base_color * i / nb_points)
    return result


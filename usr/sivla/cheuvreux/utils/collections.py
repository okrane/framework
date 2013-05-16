'''
Created on Sep 28, 2010

@author: syarc
'''


def extract(d, keys):
    ''' Extract from the dictionary 'd' the keys 'keys' and
        return a new dictionary
    '''

    return dict( (k, d[k]) for k in keys if k in d )


def invert(d):
    ''' This method invert the dictionary 'd', i.e the mapping key => value
        becomes value => key

        It will fail if the values are not unique
    '''
    return dict((v,k) for k, v in map.iteritems())



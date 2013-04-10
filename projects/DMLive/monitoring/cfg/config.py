'''
    This is the configuration file for the DMLive website.
    Contains a number of dictionary variables with the proper static configuration data
    To use import this file into your module and access the respective variables
'''

__version__ = '1.0.0'

database = {'SERVER': 'SIRIUS',
            'DATABASE': 'dm_algo',
            'ServerId': 1228,
            }

algo = {'Vwap' : {'mapping': 'vwap_view',
                  'indicator_list': [30096, 30057, 30058, 30084],
                  },
        'Twap' : {'mapping': 'twap_view',
                  'indicator_list': [30096, 30057, 30058, 30084],
                  },
        }


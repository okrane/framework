from django import forms
from monitoring.cfg.config import *

class AlgoSearchForm(forms.Form):
    client_order_id = forms.CharField(max_length=30)    
    strategy = forms.ChoiceField(choices =  (('*', '*'), 
                                            ('TAP', 'Crossfire'), 
                                            ('Vwap', 'VWAP'), 
                                            ('Twap', 'TWAP'),
                                            ('TwapCantor', 'Cantor TWAP'),
                                            ('EVPMarshallWace', 'EVP-MW'), 
                                            ('ISS', 'ISS')))
    server = forms.ChoiceField(choices =    (('*', '*'),
                                             ('PP'  ,'Pre-Production'),
                                             (str(database['ServerId'])  ,'Production', 
                                              '1228','HPPLive' )))
    
    order_id = forms.CharField(max_length=30)   
    
    
    def is_valid(self):
        return True
    
   

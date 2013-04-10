'''
Created on 11 mai 2011
@author: wehua
'''
from random import *
from simep.tactic import Tactic
from sm.Inter_S import *

class SlippageStrategy(Tactic):
    def __init__(self, setup, context, parameters):
        Tactic.__init__(self, setup, context, parameters)
        self.next_deal = 0  
        
    def process(self, evt):
        print self['Price']
        ae = self.marketManager.getFeedInfo('AE')
        if self.next_deal <= ae['C1_CONVNUM']:
            ats = int(ae['A_QTY_3'] / ae['A_NPLRS_3'] * (0.9 + random() * 0.2))
            cycle_size = ae['PARCL_SIZE'] * ae['CONV_FAC']
            venue_id = evt.getMarketId()
            TotalQty = self.moneyManager.getTotalQty()
            ExecQty = self.moneyManager.getExecQty()
            self.log('Execqty %f totalqty %f' % (ExecQty,TotalQty))
            market = self.marketManager.getFeedInfo(venue_id)
            price_bestbid = market['BEST_BID1']
            price_bestask = market['BEST_ASK1']
            tick_size = self.getReferenceData('PriceStep', venue_id = venue_id, price = price_bestbid)
            spread_size = int((price_bestask-price_bestbid)/tick_size+0.01)
            lts_index = 1
            volume_imbalance = float(market['BEST_ASIZ1']+market['BEST_ASIZ2']-market['BEST_BSIZ1']-market['BEST_BSIZ2'])/(market['BEST_ASIZ1']+market['BEST_ASIZ2']+market['BEST_BSIZ1']+market['BEST_BSIZ2']) 
            self.initialbid = price_bestbid
            self.initialask = price_bestask
            pb = self.moneyManager.getPrivateBook('SlippageOrder_Strategy')
            if pb:    
                for ord_id in pb.keys():
                    quantity     = pb[ord_id]['OrderQty']
                    self.log('Cancel Order %d' % (quantity))
                    self.cancelOccurrence(venue_id, ord_id)
                    self.moneyManager.killSM('Inter_S')

            if volume_imbalance <-0.3:
                index_imb = 0
            elif volume_imbalance <0.3:
                index_imb = 1
            else:
                index_imb = 2

            self.log('self side equals %d' % (self.side))
            if lts_index == self.side:
                if spread_size == 1:
                    ratio = 0;
                elif spread_size == 2:
                    ratio = float(self['ltb_s2'].split(',')[index_imb])
                elif spread_size == 3:
                    ratio = float(self['ltb_s3'].split(',')[index_imb])
                elif spread_size == 4:
                    ratio = float(self['ltb_s4'].split(',')[index_imb])
                else:
                    ratio = float(self['ltb'].split(',')[index_imb])
            else:
                if spread_size == 1:
                    ratio = 0;
                elif spread_size == 2:
                    ratio = float(self['lto_s2'].split(',')[index_imb])
                elif spread_size == 3:
                    ratio = float(self['lto_s3'].split(',')[index_imb])
                elif spread_size == 4:
                    ratio = float(self['lto_s4'].split(',')[index_imb])
                else:
                    ratio = float(self['lto'].split(',')[index_imb])

            quantity = min(self['MaxMultATS'] * ats,TotalQty - ExecQty)
            if ratio == 0:
                if self['Price'] == None:
                    self.log('Spread = %f ratio = %f, sending aggressive order' % (spread_size,ratio))
                    if market['BEST_OPPOSITE_SIZE1'] >= quantity:
                        oc = self.createOccurrence('MarketOrder',
                                                   venue_id = self.primary_venue_id, 
                                                   prefix = 'AggressiveOrder',
                                                   tactic_params = {'OrderQty': quantity, 
                                                                    'Price': market['BEST_OPPOSITE1'], 
                                                                    'OrderType': 'limit',
                                                                    'MarketTimeInForce': 'Fak'} 
                                                   )
                        self.log('Sending Aggressive Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                        self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.AGGRESSIVE))
                        self.target_qty = ExecQty + quantity
                        self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
                    else:
                        oc = self.createOccurrence('MarketOrder',
                                               venue_id = self.primary_venue_id, 
                                               prefix = 'AggressiveOrder',
                                               tactic_params = {'OrderQty': quantity, 
                                                                'Price': market['BEST_OPPOSITE2'], 
                                                                'OrderType': 'limit',
                                                                'MarketTimeInForce': 'Fak'} 
                                               )
                        self.log('Sending Aggressive Order to Opposite 2: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE2'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                        self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.AGGRESSIVE))
                        self.target_qty = ExecQty + quantity
                        self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
                else:
                    self.log('Spread = %f ratio = %f, sending aggressive order' % (spread_size,ratio))
                    if market['BEST_OPPOSITE_SIZE1'] >= quantity:
                        if self['Price'] * self.side > market['BEST_OPPOSITE1'] * self.side:
                            oc = self.createOccurrence('MarketOrder',
                                                       venue_id = self.primary_venue_id, 
                                                       prefix = 'AggressiveOrder',
                                                       tactic_params = {'OrderQty': quantity, 
                                                                        'Price': market['BEST_OPPOSITE1'], 
                                                                        'OrderType': 'limit',
                                                                        'MarketTimeInForce': 'Fak'} 
                                                       )
                            self.log('Sending Aggressive Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                            self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.AGGRESSIVE))
                            self.target_qty = ExecQty + quantity
                            self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
                        else:
                            self.log('Limit Price reached, wait for further update')
                    else:
                        if self['Price'] * self.side > market['BEST_OPPOSITE2'] * self.side:
                            oc = self.createOccurrence('MarketOrder',
                                                   venue_id = self.primary_venue_id, 
                                                   prefix = 'AggressiveOrder',
                                                   tactic_params = {'OrderQty': quantity, 
                                                                    'Price': market['BEST_OPPOSITE2'], 
                                                                    'OrderType': 'limit',
                                                                    'MarketTimeInForce': 'Fak'} 
                                                   )
                            self.log('Sending Aggressive Order to Opposite 2: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE2'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                            self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.AGGRESSIVE))
                            self.target_qty = ExecQty + quantity
                            self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
                        elif self['Price'] * self.side > market['BEST_OPPOSITE1'] * self.side:
                            oc = self.createOccurrence('MarketOrder',
                                                       venue_id = self.primary_venue_id, 
                                                       prefix = 'AggressiveOrder',
                                                       tactic_params = {'OrderQty': quantity, 
                                                                        'Price': market['BEST_OPPOSITE1'], 
                                                                        'OrderType': 'limit',
                                                                        'MarketTimeInForce': 'Fak'} 
                                                       )
                            self.log('Sending Aggressive Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                            self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.AGGRESSIVE))
                            self.target_qty = ExecQty + quantity
                            self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
                        else:
                            self.log('Limit Price reached, wait for further update')
            else:
                tick_change = max(min(spread_size-1,int(ratio*spread_size)),1)
                if self.side == 1:
                    price_limit = price_bestask - tick_change * tick_size
                else:
                    price_limit = price_bestbid + tick_change * tick_size
                    
                self.price_slip = price_limit
                if self['Price'] == None:
                    oc = self.createOccurrence('MarketOrder',
                                    venue_id = venue_id, 
                                    prefix = 'SlippageOrder_Strategy',
                                    tactic_params = {'OrderQty': quantity, 
                                                    'Price': price_limit, 
                                                    'OrderType': 'limit'} 
                                    )
                    self.log('spread_size = %f ratio = %f Sending limit Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (spread_size,ratio,quantity,price_limit,market['BEST_BSIZ1'],price_bestbid,market['BEST_ASIZ1'],price_bestask,self.next_deal))
                    self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.INIT))
                    self.target_qty = ExecQty + quantity
                    self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
                else:
                    if self['Price'] * self.side > price_limit * self.side:
                        oc = self.createOccurrence('MarketOrder',
                                        venue_id = venue_id, 
                                        prefix = 'SlippageOrder_Strategy',
                                        tactic_params = {'OrderQty': quantity, 
                                                        'Price': price_limit, 
                                                        'OrderType': 'limit'} 
                                        )
                        self.log('spread_size = %f ratio = %f Sending limit Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (spread_size,ratio,quantity,price_limit,market['BEST_BSIZ1'],price_bestbid,market['BEST_ASIZ1'],price_bestask,self.next_deal))
                        self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.INIT))
                        self.target_qty = ExecQty + quantity
                        self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
                    else:
                        self.log('Limit Price reached, waiting for next update')
        else:
            inter_s = self.moneyManager.getSM('Inter_S')
            if inter_s:
                if inter_s.state == Inter_S.States.AT_MID:
                    venue_id = evt.getMarketId()
                    market = self.marketManager.getFeedInfo(venue_id)
                    self.log('at mid, waiting for further decision')
                    price_bestbid = market['BEST_BID1']
                    price_bestask = market['BEST_ASK1']
                    size_bestask = market['BEST_ASIZ1']
                    size_bestask2 = market['BEST_ASIZ2']
                    size_bestbid = market['BEST_BSIZ1']
                    size_bestbid2 = market['BEST_BSIZ2']
                        
                    random_res = random()
                    if self.side == 1:
                        if price_bestbid > self.price_slip:
                            if random_res > float(size_bestask-size_bestbid-size_bestbid2)/(size_bestask+size_bestbid+size_bestbid2):
                                self.log('Imbalance Level %f' % (float(size_bestask-size_bestbid-size_bestbid2)/(size_bestask+size_bestbid+size_bestbid2)))
                                inter_s.state = Inter_S.States.AGGRESSIVE
                                self.log('change to aggressive state: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                            else:
                                self.log('Anti Gaming: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                        elif price_bestask > self.initialask:
                            if random_res > float(size_bestask-size_bestbid)/(size_bestask+size_bestbid):
                                self.log('Imbalance Level %f' % (float(size_bestask-size_bestbid)/(size_bestask+size_bestbid)))
                                inter_s.state = Inter_S.States.AGGRESSIVE
                                self.log('change to aggressive state: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                            else:
                                self.log('Anti Gaming: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                    else:
                        if price_bestask < self.price_slip:
                            if random_res > float(size_bestbid-size_bestask-size_bestask2)/(size_bestask+size_bestbid+size_bestask2):
                                self.log('Imbalance Level %f' % (float(size_bestbid-size_bestask-size_bestask2)/(size_bestask+size_bestbid+size_bestask2)))
                                inter_s.state = Inter_S.States.AGGRESSIVE
                                self.log('change to aggressive state: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                            else:
                                self.log('Anti Gaming: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                        elif price_bestbid < self.initialbid:
                            if random_res > float(size_bestbid-size_bestask)/(size_bestask+size_bestbid):
                                self.log('Imbalance Level %f' % (float(size_bestbid-size_bestask)/(size_bestask+size_bestbid)))
                                inter_s.state = Inter_S.States.AGGRESSIVE
                                self.log('change to aggressive state: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                            else:
                                self.log('Anti Gaming: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                        
                if inter_s.state == Inter_S.States.AGGRESSIVE:
                    venue_id = evt.getMarketId()
                    market = self.marketManager.getFeedInfo(venue_id)
                    TotalQty = self.moneyManager.getTotalQty()
                    ExecQty = self.moneyManager.getExecQty()
                    pb = self.moneyManager.getPrivateBook('SlippageOrder_Strategy')
                    if pb:    
                        for ord_id in pb.keys():
                            quantity     = pb[ord_id]['OrderQty']
                            self.log('Cancel Order %d' % (quantity))
                            self.cancelOccurrence(venue_id, ord_id)
                    if ExecQty < self.target_qty:
                        quantity = self.target_qty - ExecQty
                        if self['Price'] == None:
                            if market['BEST_OPPOSITE_SIZE1'] >= quantity: 
                                oc = self.createOccurrence('MarketOrder',
                                                           venue_id = venue_id, 
                                                           prefix = 'AggressiveOrder',
                                                           tactic_params = {'OrderQty': quantity, 
                                                                            'Price': market['BEST_OPPOSITE1'], 
                                                                            'OrderType': 'limit',
                                                                            'MarketTimeInForce': 'Fak'} 
                                                           )
                            
                                inter_s.setOrderName(oc)
                                self.log('Sending Aggressive Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                            else:
                                oc = self.createOccurrence('MarketOrder',
                                                           venue_id = venue_id, 
                                                           prefix = 'AggressiveOrder',
                                                           tactic_params = {'OrderQty': quantity, 
                                                                            'Price': market['BEST_OPPOSITE2'], 
                                                                            'OrderType': 'limit',
                                                                            'MarketTimeInForce': 'Fak'} 
                                                           )
                            
                                inter_s.setOrderName(oc)
                                self.log('Sending Aggressive Order to best opposite 2: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE2'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                        else:
                            if market['BEST_OPPOSITE_SIZE1'] >= quantity: 
                                if self['Price'] * self.side > market['BEST_OPPOSITE1'] * self.side:
                                    oc = self.createOccurrence('MarketOrder',
                                                               venue_id = venue_id, 
                                                               prefix = 'AggressiveOrder',
                                                               tactic_params = {'OrderQty': quantity, 
                                                                                'Price': market['BEST_OPPOSITE1'], 
                                                                                'OrderType': 'limit',
                                                                                'MarketTimeInForce': 'Fak'} 
                                                               )
                                    
                                    inter_s.setOrderName(oc)
                                    self.log('Sending Aggressive Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                                else:
                                    self.log('Limit Price reached, wait for further update')
                            else:
                                if self['Price'] * self.side > market['BEST_OPPOSITE2'] * self.side:
                                    oc = self.createOccurrence('MarketOrder',
                                                               venue_id = venue_id, 
                                                               prefix = 'AggressiveOrder',
                                                               tactic_params = {'OrderQty': quantity, 
                                                                                'Price': market['BEST_OPPOSITE2'], 
                                                                                'OrderType': 'limit',
                                                                                'MarketTimeInForce': 'Fak'} 
                                                               )
                                    
                                    inter_s.setOrderName(oc)
                                    self.log('Sending Aggressive Order to best opposite 2: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE2'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                                elif self['Price'] * self.side > market['BEST_OPPOSITE1'] * self.side:
                                    oc = self.createOccurrence('MarketOrder',
                                                               venue_id = venue_id, 
                                                               prefix = 'AggressiveOrder',
                                                               tactic_params = {'OrderQty': quantity, 
                                                                                'Price': market['BEST_OPPOSITE1'], 
                                                                                'OrderType': 'limit',
                                                                                'MarketTimeInForce': 'Fak'} 
                                                               )
                                    
                                    inter_s.setOrderName(oc)
                                    self.log('Sending Aggressive Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                                else:
                                    self.log('Limit Price reached, wait for further update')
                                
                if inter_s.state == Inter_S.States.NOT_SENT:
                    self.log('passive order not sent, reassess the best slippage strategy')
                    ats = int(ae['A_QTY_3'] / ae['A_NPLRS_3'] * (0.9 + random() * 0.2))
                    venue_id = evt.getMarketId()
                    TotalQty = self.moneyManager.getTotalQty()
                    ExecQty = self.moneyManager.getExecQty()
                    market = self.marketManager.getFeedInfo(venue_id)
                    price_bestbid = market['BEST_BID1']
                    price_bestask = market['BEST_ASK1']
                    tick_size = self.getReferenceData('PriceStep', venue_id = venue_id, price = price_bestbid)
                    spread_size = int((price_bestask-price_bestbid)/tick_size+0.01)
                    lts_index = 0
                    volume_imbalance = float(market['BEST_ASIZ1']+market['BEST_ASIZ2']-market['BEST_BSIZ1']-market['BEST_BSIZ2'])/(market['BEST_ASIZ1']+market['BEST_ASIZ2']+market['BEST_BSIZ1']+market['BEST_BSIZ2']) 
                    self.initialbid = price_bestbid
                    self.initialask = price_bestask
                    pb = self.moneyManager.getPrivateBook('SlippageOrder_Strategy')
                    if pb:    
                        for ord_id in pb.keys():
                            quantity     = pb[ord_id]['OrderQty']
                            self.log('Cancel Order %d' % (quantity))
                            self.cancelOccurrence(venue_id, ord_id)
        
                    if volume_imbalance <-0.3:
                        index_imb = 0
                    elif volume_imbalance <0.3:
                        index_imb = 1
                    else:
                        index_imb = 2
        
                    self.log('self side equals %d' % (self.side))
                    if lts_index == self.side:
                        if spread_size == 1:
                            ratio = 0;
                        elif spread_size == 2:
                            ratio = float(self['ltb_s2'].split(',')[index_imb])
                        elif spread_size == 3:
                            ratio = float(self['ltb_s3'].split(',')[index_imb])
                        elif spread_size == 4:
                            ratio = float(self['ltb_s4'].split(',')[index_imb])
                        else:
                            ratio = float(self['ltb'].split(',')[index_imb])
                    else:
                        if spread_size == 1:
                            ratio = 0;
                        elif spread_size == 2:
                            ratio = float(self['lto_s2'].split(',')[index_imb])
                        elif spread_size == 3:
                            ratio = float(self['lto_s3'].split(',')[index_imb])
                        elif spread_size == 4:
                            ratio = float(self['lto_s4'].split(',')[index_imb])
                        else:
                            ratio = float(self['lto'].split(',')[index_imb])
                        
                    quantity = min(self['MaxMultATS'] * ats,TotalQty - ExecQty)
                    if ratio == 0:
                        if self['Price'] == None:
                            self.log('Spread = %f ratio = %f, sending aggressive order' % (spread_size,ratio))
                            if market['BEST_OPPOSITE_SIZE1'] >= quantity:
                                oc = self.createOccurrence('MarketOrder',
                                                           venue_id = self.primary_venue_id, 
                                                           prefix = 'AggressiveOrder',
                                                           tactic_params = {'OrderQty': quantity, 
                                                                            'Price': market['BEST_OPPOSITE1'], 
                                                                            'OrderType': 'limit',
                                                                            'MarketTimeInForce': 'Fak'} 
                                                           )
                                self.log('Sending Aggressive Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                                self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.AGGRESSIVE))
                                self.target_qty = ExecQty + quantity
                                self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
                            else:
                                oc = self.createOccurrence('MarketOrder',
                                                       venue_id = self.primary_venue_id, 
                                                       prefix = 'AggressiveOrder',
                                                       tactic_params = {'OrderQty': quantity, 
                                                                        'Price': market['BEST_OPPOSITE2'], 
                                                                        'OrderType': 'limit',
                                                                        'MarketTimeInForce': 'Fak'} 
                                                       )
                                self.log('Sending Aggressive Order to Opposite 2: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE2'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                                self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.AGGRESSIVE))
                                self.target_qty = ExecQty + quantity
                                self.next_deal = int(ae['C1_CONVNUM']) + cycle_size                    
                        else:
                            self.log('Spread = %f ratio = %f, sending aggressive order' % (spread_size,ratio))
                            if market['BEST_OPPOSITE_SIZE1'] >= quantity:
                                if self['Price'] * self.side > market['BEST_OPPOSITE1'] * self.side:
                                    oc = self.createOccurrence('MarketOrder',
                                                               venue_id = self.primary_venue_id, 
                                                               prefix = 'AggressiveOrder',
                                                               tactic_params = {'OrderQty': quantity, 
                                                                                'Price': market['BEST_OPPOSITE1'], 
                                                                                'OrderType': 'limit',
                                                                                'MarketTimeInForce': 'Fak'} 
                                                               )
                                    self.log('Sending Aggressive Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                                    self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.AGGRESSIVE))
                                    self.target_qty = ExecQty + quantity
                                    self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
                                else:
                                    self.log('Limit Price reached, wait for further update')
                            else:
                                if self['Price'] * self.side > market['BEST_OPPOSITE2'] * self.side:
                                    oc = self.createOccurrence('MarketOrder',
                                                           venue_id = self.primary_venue_id, 
                                                           prefix = 'AggressiveOrder',
                                                           tactic_params = {'OrderQty': quantity, 
                                                                            'Price': market['BEST_OPPOSITE2'], 
                                                                            'OrderType': 'limit',
                                                                            'MarketTimeInForce': 'Fak'} 
                                                           )
                                    self.log('Sending Aggressive Order to Opposite 2: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE2'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                                    self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.AGGRESSIVE))
                                    self.target_qty = ExecQty + quantity
                                    self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
                                elif self['Price'] * self.side > market['BEST_OPPOSITE1'] * self.side:
                                    oc = self.createOccurrence('MarketOrder',
                                                               venue_id = self.primary_venue_id, 
                                                               prefix = 'AggressiveOrder',
                                                               tactic_params = {'OrderQty': quantity, 
                                                                                'Price': market['BEST_OPPOSITE1'], 
                                                                                'OrderType': 'limit',
                                                                                'MarketTimeInForce': 'Fak'} 
                                                               )
                                    self.log('Sending Aggressive Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                                    self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.AGGRESSIVE))
                                    self.target_qty = ExecQty + quantity
                                    self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
                                else:
                                    self.log('Limit Price reached, wait for further update')
                    else:
                        tick_change = max(min(spread_size-1,int(ratio*spread_size)),1)
                        if self.side == 1:
                            price_limit = price_bestask - tick_change * tick_size
                        else:
                            price_limit = price_bestbid + tick_change * tick_size
                            
                        self.price_slip = price_limit
                        if self['Price'] == None:
                            oc = self.createOccurrence('MarketOrder',
                                            venue_id = venue_id, 
                                            prefix = 'SlippageOrder_Strategy',
                                            tactic_params = {'OrderQty': quantity, 
                                                            'Price': price_limit, 
                                                            'OrderType': 'limit'} 
                                            )
                            self.log('spread_size = %f ratio = %f Sending limit Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (spread_size,ratio,quantity,price_limit,market['BEST_BSIZ1'],price_bestbid,market['BEST_ASIZ1'],price_bestask,self.next_deal))
                            self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.INIT))
                            self.target_qty = ExecQty + quantity
                            self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
                        elif self['Price'] * self.side > price_limit * self.side:
                                oc = self.createOccurrence('MarketOrder',
                                                venue_id = venue_id, 
                                                prefix = 'SlippageOrder_Strategy',
                                                tactic_params = {'OrderQty': quantity, 
                                                                'Price': price_limit, 
                                                                'OrderType': 'limit'} 
                                                )
                                self.log('spread_size = %f ratio = %f Sending limit Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (spread_size,ratio,quantity,price_limit,market['BEST_BSIZ1'],price_bestbid,market['BEST_ASIZ1'],price_bestask,self.next_deal))
                                self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.INIT))
                                self.target_qty = ExecQty + quantity
                                self.next_deal = int(ae['C1_CONVNUM']) + cycle_size

    def processTimer(self,evt):
        self.log('Timer %s is on')
        venue_id = self.primary_venue_id
        market = self.marketManager.getFeedInfo(venue_id)
        best_opposite = market['BEST_OPPOSITE1']
        current_pb = self.moneyManager.getPrivateBook('SlippageOrder_Strategy')
        if current_pb:    
            for ord_id in current_pb.keys():
                quantity     = current_pb[ord_id]['OrderQty']
                self.log('Cancel Order %d' % (quantity))
                self.cancelOccurrence(venue_id, ord_id)
        else:
            self.log('no existing slippage order')
            
        ExecQty = self.moneyManager.getExecQty()
        if ExecQty < self.target_qty:
            quantity = self.target_qty - ExecQty
            oc = self.createOccurrence('MarketOrder',
                                       venue_id = venue_id, 
                                       prefix = 'AggressiveOrder',
                                       tactic_params = {'OrderQty': quantity, 
                                                        'Price': market['BEST_OPPOSITE1'], 
                                                        'OrderType': 'limit',
                                                        'MarketTimeInForce': 'Fak'} 
                                       )
            self.log('Sending Aggressive Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
        self.moneyManager.killSM('Inter_S')
            
    def processReport(self, evt):
        pass
    
    
    def acceptSubscription(self, sub_id, sub):        
        return sub_id == self.primary_venue_id

    def acceptVenue(self, venue_id, venue):
                
        if venue.info["type"] != "Lit":
            return False        
        if str(venue.info["destinationId"]) == str(self['destinationId']):
            self.primary_venue_id = venue_id
            self.trading_destination  = venue.info['destinationId']
            return True
        return False

    def extraSubscriptions(self):
        
        ''' Returns any extra subscriptions needed. 
            @return
                Dictionary: {name: (Source, Ric, ReqdProcess)}
        '''
        return {'AE':(self['vwapSource'], 'S=%d@%s;P=bEHRC;Z=UTC;H=4;F=%s;T=%s;EX=top;EX=cross;EX=auc' % (self['InstrumentID'], self.trading_destination, self['EffectiveTime'][9:], self['ExpireTime'][9:]) )}

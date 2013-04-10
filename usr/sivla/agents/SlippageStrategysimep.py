'''
Created on 11 mai 2011
@author: wehua
'''
from random import *
from simep.core.subtactic import Tactic

class SlippageStrategysimep(Tactic):
    def __init__(self, setup, context, parameters):
        Tactic.__init__(self, setup, context, parameters)
        self.next_deal = 0  
        self.state = 0
        
    def process(self, evt):
        ae = self.marketManager.getFeedInfo('AE')
        if self.next_deal <= ae['C1_CONVNUM'] and self.state == 0:
            ats = int(ae['A_QTY_3'] / ae['A_NPLRS_3'] * (0.9 + random() * 0.2))
            cycle_size = ae['PARCL_SIZE'] * ae['CONV_FAC']
            self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
            venue_id = evt.getVenueId()
            TotalQty = self.moneyManager.getTotalQty()
            ExecQty = self.moneyManager.getExecQty()
            self.log('Execqty %f totalqty %f' % (ExecQty,TotalQty))
            market = self.marketManager.getFeedInfo(venue_id)
            price_bestbid = market['BEST_BID1']
            price_bestask = market['BEST_ASK1']
            if price_bestbid <=9.999:
                tick_size = 0.001
            elif price_bestbid <=49.995:
                tick_size = 0.005
            elif price_bestbid <=99.99:
                tick_size = 0.01
            else:
                tick_size = 0.05
                
#            tick_size = self.getReferenceData('PriceStep', venue_id = venue_id, price = price_bestbid)
            spread_size = int((price_bestask-price_bestbid)/tick_size+0.01)
            lts_index = 1
            volume_imbalance = float(market['BEST_ASIZ1']+market['BEST_ASIZ2']-market['BEST_BSIZ1']-market['BEST_BSIZ2'])/(market['BEST_ASIZ1']+market['BEST_ASIZ2']+market['BEST_BSIZ1']+market['BEST_BSIZ2']) 
            self.initialbid = price_bestbid
            self.initialask = price_bestask
            pb = self.moneyManager.getPrivateBook('SlippageOrder_Strategy')
            if pb:    
                for ord_id in pb.keys():
                    quantity     = int(pb[ord_id]['OrderQty'])
                    self.log('Cancel Order %d' % (quantity))
#                    self.cancelOccurrence(venue_id, ord_id)
                    self.cancelOrder(venue_id, ord_id)

            if volume_imbalance <-0.3:
                index_imb = 0
            elif volume_imbalance <0.3:
                index_imb = 1
            else:
                index_imb = 2

            self.log('self side equals %d' % (self.side))
            if lts_index == self.side:
                ratio = float(self['ltb'].split(',')[index_imb])
            else:
                ratio = float(self['lto'].split(',')[index_imb])

            quantity = min(self['MaxMultATS'] * ats,TotalQty - ExecQty)
            if ratio == 0 or spread_size <= 2:
                self.log('Spread = %f ratio = %f, sending aggressive order' % (spread_size,ratio))
                if market['BEST_OPPOSITE_SIZE1'] >= quantity:
                    self.log('Before sending Aggressive Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                    self.createLimitOrder(venue_id, 
                                               market['BEST_OPPOSITE1'],
                                               quantity, 
                                               execType = 'Fak', 
                                               prefix = 'AggressiveOrder',
                                               )
                else:
                    self.log('Before sending Aggressive Order to best opposite 2: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE2'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                    self.createLimitOrder(venue_id, 
                                               market['BEST_OPPOSITE2'],
                                               quantity, 
                                               execType = 'Fak', 
                                               prefix = 'AggressiveOrder',
                                               )
#                self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.AGGRESSIVE))
                self.state = 2
                self.log('change to state aggressive')
            else:
                tick_change = max(min(spread_size-1,int(ratio*spread_size)),1)
                if self.side == 1:
                    price_limit = price_bestbid + tick_change * tick_size
                else:
                    price_limit = price_bestask - tick_change * tick_size
                    
                self.price_slip = price_limit
                
                self.createLimitOrder(venue_id, 
                                      price_limit,
                                      quantity,
                                      prefix = 'SlippageOrder_Strategy',
                                )
                self.log('spread_size = %f ratio = %f Sending limit Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (spread_size,ratio,quantity,price_limit,market['BEST_BSIZ1'],price_bestbid,market['BEST_ASIZ1'],price_bestask,self.next_deal))
#                self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.INIT))
                self.state = 1
                self.log('change to state at mid')
            self.target_qty = ExecQty + quantity
        else:
#            inter_s = self.moneyManager.getSM('Inter_S')
            ExecQty = self.moneyManager.getExecQty()
            if ExecQty >= self.target_qty:
                self.state = 0
                self.log('Quantity Finished')
            if self.state == 1:
                venue_id = evt.getVenueId()
                market = self.marketManager.getFeedInfo(venue_id)
                self.log('at mid, waiting for further decision')
                price_bestbid = market['BEST_BID1']
                price_bestask = market['BEST_ASK1']
                size_bestask = market['BEST_ASIZ1']
                size_bestbid = market['BEST_BSIZ1']
                        
                if self.side == 1:
                    if price_bestbid > self.price_slip or price_bestask > self.initialask:
                        if random() > float(size_bestask-size_bestbid)/(size_bestask+size_bestbid):
                            self.log('First Imbalance Level %f' % (float(size_bestask-size_bestbid)/(size_bestask+size_bestbid)))
                            self.state = 2
                            self.log('change to aggressive state: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                        else:
                            self.log('Anti Gaming: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                else:
                    if price_bestask < self.price_slip or price_bestbid < self.initialbid:
                        if random() > float(size_bestbid-size_bestask)/(size_bestask+size_bestbid):
                            self.log('First Imbalance Level %f' % (float(size_bestbid-size_bestask)/(size_bestask+size_bestbid)))
                            self.state = 2
                            self.log('change to aggressive state: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                        else:
                            self.log('Anti Gaming: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
            if self.state == 2:
                venue_id = evt.getVenueId()
                market = self.marketManager.getFeedInfo(venue_id)
                TotalQty = self.moneyManager.getTotalQty()
#                LeaveQty = self.moneyManager.getLeavesQty()
#                ExecQty = self.moneyManager.getExecQty()
#                print '.............'
#                print TotalQty, LeaveQty, ExecQty,self.target_qty
#                print '.............'
                pb = self.moneyManager.getPrivateBook('SlippageOrder_Strategy')
                self.log('Before Annul: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE2'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                if pb:    
                    for ord_id in pb.keys():
                        quantity     = int(pb[ord_id]['OrderQty'])
                        self.log('Cancel Order %d' % (quantity))
                        self.cancelOrder(venue_id, ord_id)
                if ExecQty < self.target_qty:
                    quantity = self.target_qty - ExecQty
                    if market['BEST_OPPOSITE_SIZE1'] >= quantity: 
                        self.log('Before Sending Aggressive Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                        self.createLimitOrder(venue_id,
                                              market['BEST_OPPOSITE1'],
                                              quantity, 
                                              execType = 'Fak', 
                                              prefix = 'AggressiveOrder',
                                              )
                    else:
                        self.log('Before Sending Aggressive Order to best opposite 2: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                        self.createLimitOrder(venue_id,
                                              market['BEST_OPPOSITE2'],
                                              quantity, 
                                              execType = 'Fak', 
                                              prefix = 'AggressiveOrder',
                                              )
    
    def processReport(self, evt):
        total_qty_so_far = self.bus.get('%s_TotalQuantity' % self['ParentAutomatonOrderID'])
        if not total_qty_so_far:
            total_qty_so_far = 0
        for x in self.moneyManager.getLastExecutions():
            self.log("Order Filled: Qty=%d Price=%f" % (int(x['Quantity']), float(x['Price'])))
            total_qty_so_far += int(x['Quantity'])
        
        self.log('Total Exec Quantity by Simep Agent: %d' % total_qty_so_far)
        self.bus.put('%s_TotalQuantity' % self['ParentAutomatonOrderID'], total_qty_so_far)
    
    
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

from manager import Manager

class VenueManager:
    class States:
        Available = 1
        Pending   = 2
        Passive   = 3
        
    def __init__(self, id, type, manager):
        self.manager = manager
        self.state = VenueManager.States.Available
        self.answers = 0
        self.id = id
        self.type = type
        self.q = 0
        self.quantity = 0
        self.price = 0
        
    def receive_quantity(self, quantity, price):
        if quantity == 0:
            return         
        if self.state == VenueManager.States.Available:
            print "Venue [%d] has received %d@%f" % (self.id, quantity, price)
            self.send_order(quantity, price)
        else:
            print "Venue %d not available for %d@%f" % (self.id, quantity, price)
           
    def reserve_quantity(self, quantity, price):
        for price in sorted([f[1] for f in self.manager.feed.keys() if (f[0]==self.id)]) :
            #print price, quantity
            old_qty = self.manager.feed[(self.id, price)]
            self.manager.feed[(self.id, price)] = max(0, old_qty - quantity)
            quantity = max(0, quantity - old_qty)
        
    def send_order(self, quantity, price):
        if self.type == 'main': # the main node propagates qty
            self.q += quantity
        else: # all others send orders to the market
            print "Sending %s -> [%d@%f]" % (self.id, quantity, price)       
            self.quantity = quantity    
            self.price = price
            self.state = VenueManager.States.Pending
            self.reserve_quantity(quantity, price)
            
    def market_answer(self, quantity):
        self.answers += 1
        self.state = VenueManager.States.Available
        self.q = self.quantity - quantity
        print "Executed %s : [%d]. Remaining: %d" % (self.id, quantity, self.q)
        if self.q == 0 and self.manager.feed[(self.id, self.price)] == 0: # for full fill with no remaining qty : iceberg
            self.manager.feed[(self.id, self.price)] = quantity
            
    def get_state(self):
        return self.state
    
    def propagate(self, venue, quantity, price):
        self.q = self.q - quantity               
        venue.receive_quantity(quantity, price)
        if self.q == 0:
            self.state = VenueManager.States.Available
        
class Order:
    def __init__(self, quantity):        
        self.quantity = quantity
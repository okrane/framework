from venue import VenueManager
class Tree:
    def __init__(self, manager):    
        self.manager = manager    
        self.links = {}        
        self.price = None
    
    def set_price(self, price):
        print "New Limit Price %f" % price
        self.price = price
    
    def inject_order(self, quantity):
        self.manager.venues[0].q = quantity
    
    def add_link(self, v1, v2, handle_q, handle_a):
        self.links[(v1, v2)] = (handle_q, handle_a)
    
    def find_links(self, v):
        return filter(lambda x: v == x[0], self.links.keys())
    
    def step(self):
        for v in sorted(self.manager.venues.keys()):
            if self.manager.venues[v].q > 0:
                for l in sorted(self.find_links(v)):
                    if self.links[(v, l[1])][1](v, l[1], self.manager.feed):
                        self.manager.venues[v].propagate(self.manager.venues[l[1]], 
                                                         self.links[(v, l[1])][0](self.manager.venues[v].q, self.manager, v, l[1], self.price ), 
                                                         self.price)
        
        print 'available venues:', len(filter(lambda x: x.get_state() == VenueManager.States.Available, self.manager.venues.values()))
        return len(self.manager.venues.values())==len(filter(lambda x: x.get_state() == VenueManager.States.Available, self.manager.venues.values()))
    
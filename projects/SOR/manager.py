class Manager():
    def __init__(self):
        self.feed = {}
        self.initial_feed = {}
        self.venues = {}
    
    def set_feed(self, feed):
        self.feed = feed
        self.initial_feed = dict(feed)
    
    def find_price(self, quantity):
        def comp(x, y):
            k = 1 if (x[1]-y[1])==0 else 0
            result = int(10000*(x[1]-y[1])) + k * (x[0] - y[0])
            return result        
        q = 0
        p = 0
        for (a, b) in sorted(self.feed.keys(), comp, reverse = False):                    
            q += self.feed[(a, b)]
            p = b
            if q >= quantity:
                break
        return p
    
    def volume_at_price(self, price, venue = None):
        if venue:
            return sum([feed[(venue, x)] for x in [y[1] for y in self.feed.keys() if y[1] <= price]])
        else:
             return sum([self.feed[y] for y in self.feed.keys() if y[1] <= price])
    

    
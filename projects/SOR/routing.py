
from usr.dev.sivla.funcs.SOR.venue import *
from usr.dev.sivla.funcs.SOR.manager import *
from usr.dev.sivla.funcs.SOR.Tree import *

""" Mode Sweep """
manager = Manager()
manager.venues = {0 : VenueManager(0, 'main', manager),
                 1 : VenueManager(1, 'fak', manager),
                 2 : VenueManager(2, 'fak', manager),
                 3 : VenueManager(3, 'limit', manager),          
                 }

tree = Tree(manager)

def dispatch_fak(q, manager, v_in, v_out, price):   
   #print sorted([y[1] for y in feed.keys() if (y[1] <= price and y[0] == v_out)])
   #print [manager.feed[(v_out, x)] for x in sorted( [y[1] for y in feed.keys() if (y[1] <= price and y[0] == v_out)])]
   max_q = sum( manager.feed[(v_out, x)] for x in sorted( [y[1] for y in manager.feed.keys() if (y[1] <= price and y[0] == v_out)]))   
   print 'Max Quantity at Price %f for venue %d: %d' % (price, v_out, max_q)
   if manager.venues[v_out].get_state() == VenueManager.States.Available:
       return min(q, max_q)
   else: 
       return 0

def active_fak(v_in, v_out, feed):
   return manager.venues[v_out].answers <= 2

def dispatch_limit(q, manager, v_in, v_out, price):
   return q

def active_limit(v_in, v_out, feed):
   return sum(feed.values()) <= 0

def dispatch_up(q, manager, v_in, v_out, price):
   return q

def active_up(v_in, v_out, feed):
   return True 

   
tree.add_link(0, 1, dispatch_fak, active_fak)
tree.add_link(0, 2, dispatch_fak, active_fak)
tree.add_link(0, 3, dispatch_fak, active_fak)
tree.add_link(1, 0, dispatch_up, active_up)
tree.add_link(2, 0, dispatch_up, active_up)
tree.add_link(3, 0, dispatch_up, active_up)

feed = {(1, 9.54): 1000, 
        (2, 9.55): 300, 
        (3, 9.56): 300, 
        (1, 9.56): 300, 
        (1, 9.57): 200, 
        (3, 9.58): 200, 
        (2, 9.58): 200, 
        (1, 9.59): 400, 
        (2, 9.59): 100, 
        (3, 9.60): 400, 
        (1, 9.60): 250, 
        (2, 9.60): 200, 
        (2, 9.61): 100, 
        (3, 9.62): 250}

manager.set_feed(feed)

order_quantity = 10000
order_price    = 9.59

p = manager.find_price(order_quantity)
if p < order_price:
    tree.set_price(p)
else:
    tree.set_price(order_price)
        
tree.inject_order(order_quantity)

 
tree.step()
print "==========================="
manager.venues[1].market_answer(0)
tree.step()
tree.step()
print "==========================="
manager.venues[2].market_answer(600)
tree.step()
tree.step()
print "==========================="
manager.venues[3].market_answer(200)
tree.step()
tree.step()
print "==========================="
manager.venues[2].market_answer(600)
tree.step()
tree.step()
print "==========================="
manager.venues[2].market_answer(600)
tree.step()
tree.step()



#===============================================================================
# """ Mode Rifle """
# venues = {0 : VenueManager(0, 'main'),
#          1 : VenueManager(1, 'fak'),
#          2 : VenueManager(2, 'fak'),
#          3 : VenueManager(3, 'limit'),          
#          }
# 
# tree = Tree(venues)
# 
# def dispatch_fak(q, feed, v_in, v_out):
#    return q
# 
# def active_fak(v_in, v_out):
#    return True
# 
# def dispatch_limit(q, feed, v_in, v_out):
#    return q
# 
# def active_limit(v_in, v_out):
#    return True
# 
# def dispatch_up(q, feed, v_in, v_out):
#    return q
# 
# def active_up(v_in, v_out):
#    return True 
# 
# tree.add_link(0, 1, dispatch_fak, active_fak)
# tree.add_link(1, 2, dispatch_fak, active_fak)
# tree.add_link(2, 3, dispatch_limit, active_limit)
# 
# feed = {1: 30, 2: 30, 3: 0}
# 
# tree.inject_feed(feed)
# tree.inject_order(80)
# 
# tree.step()
# print "------------"
# tree.execute(1, 20)
# print "------------"
# tree.step()
# print "------------"
#===============================================================================
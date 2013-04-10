
class AlternationCalculator():
      # Simple Helper class to compute alternation and continuation
     def __init__(self, last_move, nb_alternation, nb_continuation):
       self.reset_values(last_move, nb_alternation, nb_continuation)
       self.spread_is_crossed = False  
     def reset_values(self, last_value, nb_alternation, nb_continuation):
          self.nb_alternation = 0
          self.nb_continuation = 0
          self.previous_move =  last_value
     
     def compute_alternation(self, current_value, last_value):
        moves = self.get_alternation_value(last_value,current_value)
       # print "new value : %.4f , old value : %.4f, move : %d"%(current_value, last_value,moves)
            
        if moves  != 0 and self.previous_move != None:
            if self.previous_move * moves  == 1:
                #2 up (1 * 1)  or 2 down --1 * -1)-> continuation
               self.nb_continuation += 1
            elif self.previous_move*moves  == -1:
                #1 up and 2 down -> continuation
                self.nb_alternation += 1
        self.previous_move  = moves 
    
     def get_alternation_value(self, old_value, new_value):
        if old_value == new_value:
            return 0 # no move
        elif old_value < new_value:
            return 1 #move up
        else: 
            return -1 #move down   
    
    
class QuadraticCalculator():
     # Simple Helper class to compute alternation and continuation
     def __init__(self, last_move):
       self.reset_values(last_move)
       
     def reset_values(self, last_value):
          self.quad_sum = 0
          self.last_value=  last_value
     
     def compute_quadratic_variation(self, current_value, last_value):
        
        if self.last_value == None and last_value != None:
            # first call, just init the system
            self.last_value = last_value
        
        if current_value != None and self.last_value != None:
            self.quad_sum += (current_value - self.last_value)*(current_value - self.last_value)
            self.last_value = current_value
            return self.quad_sum
        
     def get_current_quadratic_variation(self):
         return  self.quad_sum
            
def sort_by_value(d):
    items=d.items()
    backitems=[ [v[1],v[0]] for v in items]
    backitems.sort()
    return [ backitems[i][1] for i in range(0,len(backitems))]
    
def get_max_value_key_in_dict(a):
    b = dict(map(lambda item: (item[1],item[0]),a.items()))
    min_key = b[max(b.keys())]
    return min_key
            
        
        
        
        
        
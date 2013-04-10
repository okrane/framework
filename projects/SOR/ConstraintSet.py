'''
Created on Jun 28, 2012

@author: Silviu
'''
import math

class Constraint:
    def __init__(self, venue, type, factor, value):
        self.venue = venue  # the venue id (a number which is also the ranking of the venue)
        self.type = type    # one of "min" or "max"
        self.factor= factor # one of "quantity" or "percentage"
        self.value = value  # the value of the constraint
        
    def __str__(self):
        return "[%d] %s-%s = %d" % (self.venue, self.type, self.factor, self.value)
        
class ConstraintSet:
    def __init__(self):
        self.set = []
        self.venues = []
        
    def addConstraint(self, constraint):
        self.set.append(constraint)
        
    def addVenue(self, venue):
        if venue not in self.venues: self.venues.append(venue)
        
    def getVenueList(self):
        return self.venues    
    
    def passiveSplit(self, size):
        # first make an even split on all available venues
        split = dict()
        venues = sorted(self.getVenueList())
        for e in venues: split[e] = size / len(venues)        
        # correct for fractional quantity
        split[venues[0]] += size - sum([split[e] for e in venues]) 
        
        print "Initial Uniform Split %s" % split
        
        # next build a restrained constraint set with just 1 min and 1 max constraint per venue
        constraintSetMin = []
        constraintSetMax = []
        for v in venues:
            # get all min constraints and keep only the greatest quantity
            minconstraints = filter(lambda x: x.venue == v and x.type == 'min', self.set)
            minquantities  = [x.value if x.factor == 'quantity' else 1.0 * x.value / 100 * size for x in minconstraints ]
            if minconstraints:
                constraintSetMin.append( minconstraints[minquantities.index(max(minquantities))])
            
            # get all max constraints and keep only the smallest quantity
            maxconstraints = filter(lambda x: x.venue == v and x.type == 'max', self.set)
            maxquantities  = [x.value if x.factor == 'quantity' else 1.0 * x.value / 100 * size for x in maxconstraints ]
            if maxconstraints:
                constraintSetMax.append( maxconstraints[maxquantities.index(min(maxquantities))])
        
        # sort the min constraints in function of venue rankings and apply them one by one
        constraintSetMin.sort(key=lambda c: c.venue, reverse=False)
        
        print "Filtered and Sorted Min Constraints:"
        for c in constraintSetMin: print c
        print "===================================="
        
        treatedConstraints = []
        for x in constraintSetMin:
            print "Applying Constraint: %s" % x
            
            quantity = x.value if x.factor == 'quantity' else 1.0 * x.value / 100 * size
            if split[x.venue] < quantity:
                # try to free some quantity
                freeVenues = [v for v in venues if (v not in treatedConstraints and v != x.venue)]                
                if sum ( split[e] for e in freeVenues ) >= quantity - split[x.venue] :
                    # can serve this constraint                    
                    for e in freeVenues: split[e] = split[e] - float(quantity - split[x.venue]) / len(freeVenues)                    
                    split[x.venue] = quantity
                    split[freeVenues[0]] += size - sum([split[e] for e in venues]) 
                    treatedConstraints.append(x.venue)                    
                    print "Constraint Successful: %s" % split                    
                else:
                    # cannot serve this constraint
                    tosplit = split[x.venue]
                    split[x.venue] = 0
                    nonnullvenues = [v for v in split.keys() if split[v] > 0]
                    for v in nonnullvenues : split[v] += math.floor(1.0 * tosplit / len(nonnullvenues))
                    split[nonnullvenues[0]] += size - sum([split[e] for e in venues])
                    print "Constraint Failed: %s" % split
            else:
                treatedConstraints.append(x.venue)
                print "Constraint Already Met: %s" % split
                
            print "===================================="
                    
        constraintSetMax.sort(key=lambda c: c.venue, reverse=True)
        treatedConstraints = []
        
        print "Filtered and Sorted Max Constraints:"
        for c in constraintSetMax: print c
        print "===================================="
        for x in constraintSetMax:
            print "Applying Constraint: %s" % x            
            quantity = x.value if x.factor == 'quantity' else 1.0 * x.value / 100 * size
            
            if split[x.venue] > quantity:
                # redistribute some quantity on non-null venues
                freeVenues = [v for v in venues if (v not in treatedConstraints and v != x.venue) and split[v] > 0]                              
                if freeVenues:
                    # can redistribute: equally add it to non-null venues
                    todistrib = split[x.venue] - quantity
                    for v in freeVenues : split[v] += math.floor(1.0 * todistrib / len(freeVenues))                    
                    split[x.venue] = quantity                    
                    split[freeVenues[0]] += size - sum([split[e] for e in venues]) 
                    print "Constraint Successful: %s" % split
                else:
                    # cannot redistribute (?? either empty the venue or just leave it alone )
                    print "Constraint Failed: %s" % split
            else:
                treatedConstraints.append(x.venue)                
                print "Constraint Already Met: %s" % split
            
            treatedConstraints.append(x.venue)
            print "===================================="
            
                
        return split
    
    
    
if __name__ == '__main__':
    print "                                    "
    print "========== 1st Example ============="    
    print "                                    "
            
    ruleset = ConstraintSet()
    ruleset.addVenue(1)
    ruleset.addVenue(2)
    
    ruleset.addConstraint(Constraint(1, 'min', 'quantity', 200))
    ruleset.addConstraint(Constraint(1, 'max', 'percent', 50))
    ruleset.addConstraint(Constraint(2, 'max', 'percent', 30))
    
    print ruleset.passiveSplit(300)
    
    print "                                    "
    print "========== 2nd Example ============="    
    print "                                    "
    
    ruleset = ConstraintSet()
    ruleset.addVenue(1)
    ruleset.addVenue(2)
    
    ruleset.addConstraint(Constraint(1, 'min', 'quantity', 200))
    ruleset.addConstraint(Constraint(1, 'max', 'percent', 50))
    ruleset.addConstraint(Constraint(2, 'max', 'percent', 30))
    
    print ruleset.passiveSplit(500)
    
    
    print "                                    "
    print "========== 3rd Example ============="    
    print "                                    "
    
    ruleset = ConstraintSet()
    ruleset.addVenue(1)
    ruleset.addVenue(2)
    ruleset.addVenue(3)
    
    ruleset.addConstraint(Constraint(1, 'min', 'quantity', 200))
    ruleset.addConstraint(Constraint(1, 'max', 'percent', 50))
    ruleset.addConstraint(Constraint(3, 'min', 'quantity', 200))
    
    print ruleset.passiveSplit(200)
    
    print "                                    "
    print "========== 4th Example ============="    
    print "                                    "
    
    ruleset = ConstraintSet()
    ruleset.addVenue(1)
    ruleset.addVenue(2)
    ruleset.addVenue(3)
    
    ruleset.addConstraint(Constraint(1, 'min', 'quantity', 200))
    ruleset.addConstraint(Constraint(1, 'max', 'percent', 50))
    ruleset.addConstraint(Constraint(3, 'min', 'quantity', 200))
    ruleset.addConstraint(Constraint(2, 'min', 'quantity', 100))
    
    print ruleset.passiveSplit(400)
    
    
    print "                                    "
    print "========== 5th Example ============="    
    print "                                    "
    
    ruleset = ConstraintSet()
    ruleset.addVenue(1)
    ruleset.addVenue(2)
    ruleset.addVenue(3)
    
    ruleset.addConstraint(Constraint(1, 'min', 'quantity', 200))    
    ruleset.addConstraint(Constraint(3, 'min', 'quantity', 200))
    ruleset.addConstraint(Constraint(2, 'min', 'quantity', 100))
    ruleset.addConstraint(Constraint(1, 'min', 'percent', 50))
    ruleset.addConstraint(Constraint(2, 'min', 'percent', 40))
    
    print ruleset.passiveSplit(400)
    
    print "                                    "
    print "========== 6th Example ============="    
    print "                                    "
    
    ruleset = ConstraintSet()
    ruleset.addVenue(1)
    ruleset.addVenue(2)
    ruleset.addVenue(3)
        
    ruleset.addConstraint(Constraint(1, 'max', 'percent', 50))
    ruleset.addConstraint(Constraint(2, 'max', 'percent', 40))
    ruleset.addConstraint(Constraint(3, 'max', 'percent', 10))
    
    print ruleset.passiveSplit(400)
                
                
    print "                                    "
    print "========== 7th Example ============="    
    print "                                    "
    
    ruleset = ConstraintSet()
    ruleset.addVenue(1)
    ruleset.addVenue(2)
    ruleset.addVenue(3)
        
    ruleset.addConstraint(Constraint(1, 'min', 'percent', 20))
    ruleset.addConstraint(Constraint(2, 'min', 'percent', 20))
    ruleset.addConstraint(Constraint(3, 'min', 'percent', 60))
    
    print ruleset.passiveSplit(400)
    
    
    print "                                    "
    print "========== 8th Example ============="    
    print "                                    "
    
    ruleset = ConstraintSet()
    ruleset.addVenue(1)
    ruleset.addVenue(2)
    ruleset.addVenue(3)
        
    ruleset.addConstraint(Constraint(1, 'min', 'percent', 70))
    ruleset.addConstraint(Constraint(2, 'min', 'percent', 30))
    ruleset.addConstraint(Constraint(3, 'min', 'percent', 20))
    
    print ruleset.passiveSplit(1000)
    
    
    
                    
                
                
                
                
            
             
            
             
            
            
        


import unittest
from threading import Thread
import time

from simep.sched import Scheduler
from simep.observer import Observer


class TestThread(unittest.TestCase):

    def test_threadSimple(self):
        sched = Scheduler()
        sched.addOrderBook('normal','TestRIC')
        sched.addAgent(Observer('agent'))
        # Since we only added an Observer agent, the simulation
        # loop should remain stuck in idle state. To avoid being
        # blocked we start Scheduler.run() in a separate thread
        t = Thread(target=sched.run)
        t.start()
        
        # Now we sleep for 0.1 seconds just to give the
        # thread the time necessary for its initialization
        time.sleep(0.1)
        
        # This forces the simulation to stop
        sched.stop()

        # join() wait for the thread to terminate so we can be
        # sure that the simulation has really stopped
        t.join()
        
        

import unittest
import os, sys, inspect
from threading import Thread
import time

import usr.tests.FIX.fix_client as fixClient

from simep.sched import Scheduler
from simep.fixagent import FIXAgent
from simep.agents.observer import Observer
from simep.funcs.stdio.utils import writeCSV

class TestFIXProtocol(unittest.TestCase):

    def cleanFixFiles(self):
        cleaned = False

        while not cleaned:
            try:
                log_dir = os.listdir('./FIX/Log')
                store_dir = os.listdir('./FIX/Store')

                for file in log_dir:
                    if not os.path.isdir("./FIX/Log/" + file):
                        os.remove("./FIX/Log/" + file)
                for file in store_dir:
                    if not os.path.isdir("./FIX/Store/" + file):
                        os.remove("./FIX/Store/" + file)
                cleaned = True
            except OSError as (errno, strerror):
                print "I/O Error ({0}) : {1}".format(errno, strerror) 
                time.sleep(0.5)

    def init_fix_test(self):
        self.cleanFixFiles()

        # scheduler initialization
        sched = Scheduler()
        sched.addOrderBook('normal', 'FR0000120404')
        sched.addAgent(Observer('agent'))

        # try to create and start the FIX agent
        fixserver = FIXAgent()
        configureFixAgent(fixserver)
        fixserver.startFIX()

        # add the fix agent to the server
        sched.addAgent(fixserver)

        # start the scheduler in a thread
        t = Thread(target=sched.run)
        t.start()
        time.sleep(0.1)

        return sched, fixserver

    def stop_fix_test(self, sched, fixserver):
        fixserver.stopFIX(True)
        fixserver.closeFIX()
        sched.stop()    
        
    def export(self, sched, filename):
        obs = sched.getAgent('agent')
        tmp = Observer(obs.name)
        tmp.this = obs.this
        obs = tmp
        writeCSV(filename, obs.reportGen('FR0000120404'))

#####################
##Tests
#####################
    def test_FixLO01(self):
        if os.name == "posix":
            print "FIX can't be tested on Linux"
            self.assert_(False)
            return

        try:
            # initialize simep with a FIX agent
            sched, fix = self.init_fix_test()
            self.assert_(fixClient.test_limitOrder01())
            self.export(sched, './' + inspect.stack()[0][3] + '.csv')
        except Exception, msg:
            raise ValueError, msg
        finally:
            self.stop_fix_test(sched, fix)
            

    def test_FixLO02(self):
        if os.name == "posix":
            print "FIX can't be tested on Linux"
            self.assert_(False)
            return
        
        try:
            # initialize simep with a FIX agent
            sched, fix = self.init_fix_test()
            self.assert_(fixClient.test_limitOrder02())
            self.export(sched, './' + inspect.stack()[0][3] + '.csv')
        except Exception, msg:
            raise ValueError, msg
        finally:
            self.stop_fix_test(sched, fix)

    def test_FixLO03(self):
        if os.name == "posix":
            print "FIX can't be tested on Linux"
            self.assert_(False)
            return
        
        try:
            # initialize simep with a FIX agent
            sched, fix = self.init_fix_test()
            self.assert_(fixClient.test_limitOrder03())
            self.export(sched, './' + inspect.stack()[0][3] + '.csv')
        except Exception, msg:
            raise ValueError, msg
        finally:
            self.stop_fix_test(sched, fix)

    def test_FixLO04(self):
        if os.name == "posix":
            print "FIX can't be tested on Linux"
            self.assert_(False)
            return
        
        try:
            # initialize simep with a FIX agent
            sched, fix = self.init_fix_test()
            self.assert_(fixClient.test_limitOrder04())
            self.export(sched, './' + inspect.stack()[0][3] + '.csv')
        except Exception, msg:
            raise ValueError, msg
        finally:
            self.stop_fix_test(sched, fix)
    def test_FixLO05(self):
        if os.name == "posix":
            print "FIX can't be tested on Linux"
            self.assert_(False)
            return
        
        try:
            # initialize simep with a FIX agent
            sched, fix = self.init_fix_test()
            self.assert_(fixClient.test_limitOrder05())
            self.export(sched, './' + inspect.stack()[0][3] + '.csv')
        except Exception, msg:
            raise ValueError, msg
        finally:
            self.stop_fix_test(sched, fix)
    def test_FixMO01(self):
        if os.name == "posix":
            print "FIX can't be tested on Linux"
            self.assert_(False)
            return
        
        try:
            # initialize simep with a FIX agent
            sched, fix = self.init_fix_test()
            self.assert_(fixClient.test_marketOrder01())
            self.export(sched, './' + inspect.stack()[0][3] + '.csv')
        except Exception, msg:
            raise ValueError, msg
        finally:
            self.stop_fix_test(sched, fix)
    def test_FixMO02(self):
        if os.name == "posix":
            print "FIX can't be tested on Linux"
            self.assert_(False)
            return
        
        try:
            # initialize simep with a FIX agent
            sched, fix = self.init_fix_test()
            self.assert_(fixClient.test_marketOrder02())
            self.export(sched, './' + inspect.stack()[0][3] + '.csv')
        except Exception, msg:
            raise ValueError, msg
        finally:
            self.stop_fix_test(sched, fix)

    def test_FixMO03(self):
        if os.name == "posix":
            print "FIX can't be tested on Linux"
            self.assert_(False)
            return
        
        try:
            # initialize simep with a FIX agent
            sched, fix = self.init_fix_test()
            self.assert_(fixClient.test_marketOrder03())
            self.export(sched, './' + inspect.stack()[0][3] + '.csv')
        except Exception, msg:
            raise ValueError, msg
        finally:
            self.stop_fix_test(sched, fix)

    def test_FixSO01(self):
        if os.name == "posix":
            print "FIX can't be tested on Linux"
            self.assert_(False)
            return
        
        try:
            # initialize simep with a FIX agent
            sched, fix = self.init_fix_test()
            self.assert_(fixClient.test_stopOrder01())
            self.export(sched, './' + inspect.stack()[0][3] + '.csv')
        except Exception, msg:
            raise ValueError, msg
        finally:
            self.stop_fix_test(sched, fix)
            
    def test_FixSO02(self):
        if os.name == "posix":
            print "FIX can't be tested on Linux"
            self.assert_(False)
            return
        
        try:
            # initialize simep with a FIX agent
            sched, fix = self.init_fix_test()
            self.assert_(fixClient.test_stopOrder02())
            self.export(sched, './' + inspect.stack()[0][3] + '.csv')
        except Exception, msg:
            raise ValueError, msg
        finally:
            self.stop_fix_test(sched, fix)
            
    def test_FixSO03(self):
        if os.name == "posix":
            print "FIX can't be tested on Linux"
            self.assert_(False)
            return
        
        try:
            # initialize simep with a FIX agent
            sched, fix = self.init_fix_test()
            self.assert_(fixClient.test_stopOrder03())
            self.export(sched, './' + inspect.stack()[0][3] + '.csv')
        except Exception, msg:
            raise ValueError, msg
        finally:
            self.stop_fix_test(sched, fix)

def configureFixAgent(agent):   
    # setting the default parameters for all sessions ([DEFAULT] section of config)
    agent.setParameter("ConnectionType", "acceptor")
    agent.setParameter("SocketAcceptPort", "5003")
    agent.setParameter("SocketReuseAddress", "Y")
    agent.setParameter("FileStorePath", "./FIX/Store")
    agent.setParameter("FileLogPath", "./FIX/Log")
    agent.setParameter("StartTime", "00:00:00")
    agent.setParameter("EndTime", "00:00:00")
    agent.setParameter("DataDictionary", "./FIX/Dictionary/FIX42.xml")
    agent.setParameter("CheckLatency", "N")

    # agent.setSenderID("Simep") #Uncomment and change this line to change the FIX identifier of the agent.
    
    sess_id = agent.registerSession("FIX.4.2", "CLIENT1")
    # gent.setParameter("parameter", "value", sess_id) #uncomment and change this line to set up session-specific parameters
    
    return agent.initFIX()

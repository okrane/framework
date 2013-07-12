import unittest
import filecmp

from simep.sched import Scheduler
from simep.lightmodel import LightModel
from simep.pbgmodel import PBGModel
from simep.agents.observer import Observer
from simep.funcs.stdio.utils import writeCSV

testBasePath1 = './testreference/trades1.csv'
testBasePath2 = './testreference/trades2.csv'
outPath1 = './outfiles/trades1.csv'
outPath2 = './outfiles/trades2.csv'

class TestGlobal(unittest.TestCase):

    def runTest1(self):
        # initialise le scheduler
        sched = Scheduler()
        #sched.setSimulationSpeed(1)
        sched.setRandomGenerator(42)
        sched.addOrderBook('normal', 'TestRIC')

        # ajoute l'agent simulation au scheduler
        pbg = PBGModel('PBG', 'TestRIC')
        sched.addAgent(pbg)
        pbg.setLogin('juraz')
        pbg.setFull(False)
        pbg.setDate('20090202')
        pbg.setNbBT(10)
        pbg.setInputFileName('C:/histo/lobTrade_26_20090202.binary')
        pbg.needExecReportEvt = False
        pbg.needAllEvts = False
        
        # ajoute l'agent python "agent" au scheduler
        obs = Observer('agent')
        sched.addAgent(obs)

        sched.run()
        writeCSV('./outfiles/trades1.csv', obs.reportGen('TestRIC'))

    def runTest2(self):
        # initialise le scheduler
        sched = Scheduler()
        #sched.setSimulationSpeed(1)
        sched.setRandomGenerator(42)
        sched.addOrderBook('light', 'TestRIC')

        # ajoute l'agent simulation au scheduler
        light = LightModel('LiModl', 'TestRIC')
        sched.addAgent(light)
        
        light.setLogin('juraz')
        light.setFull(True)
        light.setDate('20090202')
        light.setInputFileName('C:/histo/lobTrade_26_20090202.binary')
        light.needExecReportEvt = False
        light.needAllEvts = False

        # ajoute l'agent python "agent" au scheduler
        obs = Observer('agent')
        sched.addAgent(obs)        
        sched.run()
        
        writeCSV('./outfiles/trades2.csv', obs.reportGen('TestRIC'))
        
    def test_ModelPBG_2times(self):
        self.runTest1()
        self.assert_(filecmp.cmp(testBasePath1, outPath1))
        self.runTest1()
        self.assert_(filecmp.cmp(testBasePath1, outPath1))
        
    def test_ModelPBG(self):
        self.runTest1()
        self.assert_(filecmp.cmp(testBasePath1, outPath1))

    def test_LightModel(self):
        self.runTest2()        
        self.assert_(filecmp.cmp(testBasePath2, outPath2))
         

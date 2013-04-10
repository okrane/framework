import simep
from simep.scenarii.scenario import Scenario
from simep.funcs.dbtools.repository import Repository

testCycle = Scenario('Cycle_Light', Repository('C:/st_sim/simep/projects/databases/repository'), 'C:/test_simep')
testCycle.loadSimulations('C:/test_simep', ".*\.py");
a = testCycle.runSimulations()
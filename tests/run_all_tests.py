import unittest
import sys

test_module_list = [
    "test_global",
    "test_bus",
    "test_orders",
    "test_fix_agent",
    "test_rfa_agent"
]

class Logger:
    def __init__(self, fd):
        self.fd = fd

    def write(self, data):
        sys.stderr.write(data)
        self.fd.write(data)

    def flush(self):
        sys.stderr.flush()
        self.fd.flush()

def buildTestSuite(module_list):
    suite = unittest.TestSuite()
    for i in module_list:
        module = __import__(i,globals(),locals())
        suite.addTest(unittest.defaultTestLoader.loadTestsFromModule(module))
    return suite

def main():
	f = open('./testresults.txt','w')
	logger = Logger(f)
	suite = buildTestSuite(test_module_list)
	unittest.TextTestRunner(logger, verbosity=2).run(suite)
	f.close()
	
if __name__ == "__main__":
	main()



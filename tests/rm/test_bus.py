import unittest
import sys
from simep.sched import Bus

class DummyObject:
    pass

class TestBus(unittest.TestCase):

    def setUp(self):
        self.bus = Bus()

    def test_intValue(self):
        self.bus.put("int", 133)
        self.assertEqual(self.bus.get("int"), 133)

    def test_doubleValue(self):
        self.bus.put("double", 99.99)
        self.assertEqual(self.bus.get("double"), 99.99)

    def test_stringValue(self):
        self.bus.put("string", "test string")
        self.assertEqual(self.bus.get("string"), "test string")
    
    def test_pyObjectValue(self):
        o = DummyObject()
        self.bus.put("Object", o)
        self.assertEqual(self.bus.get("Object"), o)
    
    def test_valueOverwrite(self):
        o = DummyObject()
        self.bus.put("value", o)
        self.bus.put("value", 88)
        self.assertEqual(self.bus.get("value"), 88)
    
    def test_valueNotFound(self):
        self.bus.put("dummy", 77)
        self.assertEqual(self.bus.get("value"), None)
    
    def test_refCount(self):
        o = DummyObject()
        cnt = sys.getrefcount(o) 
        self.bus.put("Object", o)
        self.assertEqual(sys.getrefcount(o), cnt + 1)
        o2 = self.bus.get("Object")
        self.assertEqual(sys.getrefcount(o2), cnt + 2)
        del o2
        o3 = DummyObject()
        self.bus.put("Object", o3)
        self.assertEqual(sys.getrefcount(o), cnt)
    
    def test_refCount2(self):
        o = DummyObject()
        cnt = sys.getrefcount(o) 
        self.bus.put("Object", o)
        self.bus.put("Object", 23)
        self.assertEqual(sys.getrefcount(o), cnt)
    
    def test_clear(self):
        o = DummyObject()
        cnt = sys.getrefcount(o)
        self.bus.put("Object", o)
        self.bus.put("int", 133)
        self.bus.put("double", 99.99)
        self.bus.put("string", "test string")
        self.bus.clear()
        self.assertEqual(sys.getrefcount(o), cnt)
        self.assertEqual(self.bus.get("Object"), None)
        self.assertEqual(self.bus.get("int"), None)
        self.assertEqual(self.bus.get("double"), None)
        self.assertEqual(self.bus.get("string"), None)

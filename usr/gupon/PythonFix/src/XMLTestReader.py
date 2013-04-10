from xml.etree import ElementTree
from pprint import pprint
from inspect import isclass
# calling example
#def main():
#    configdict = ConvertXmlToDict('./xml/test.xml')
#    standard_dict = configdict.UnWrap()
#    
#    pprint(standard_dict)
#    test_data = standard_dict["test"]
#    steps =test_data["step"]
#    step = 0 
#    for test_step in steps :
#        step += 1
#        has_ob  = False
#        if test_step.has_key("time") :
#            print "Step : %d, time -> %s"%(step, test_step["time"])
#        
#        if test_step.has_key("venue") :
#            obs =test_step["venue"] 
#            has_ob  = True
#            if isinstance(obs, list):
#                print "this step contains %d OB :"%(len(obs))
#                for ob in obs :
#                    print "OB for venue_id :%s"%ob["id"]
#                    if ob.has_key("orderbook") :
#                        print "OrderBook : %s"%ob["orderbook"]
#            elif isinstance(obs, dict):
#                print "OB for venue_id :%s"%obs["id"]
#                if ob.has_key("orderbook") :
#                    print "OrderBook : %s"%ob["orderbook"]
#                        
#                        
#            
#    a = 1
    
    

    # you can access the data as a dictionary
#    print configdict['settings']['color']
#    configdict['settings']['color'] = 'red'
#
#    # or you can access it like object attributes
#    print configdict.settings.color
#    configdict.settings.color = 'red'
#
#    root = ConvertDictToXml(configdict)
#
#    tree = ElementTree.ElementTree(root)
#    tree.write('config.new.xml')


# Module Code:

class XmlDictObject(dict):
    """
    Adds object like functionality to the standard dictionary.
    """

    def __init__(self, initdict=None):
        if initdict is None:
            initdict = {}
        dict.__init__(self, initdict)
    
    def __getattr__(self, item):
        return self.__getitem__(item)
    
    def __setattr__(self, item, value):
        self.__setitem__(item, value)
    
    def __str__(self):
        if self.has_key('_text'):
            return self.__getitem__('_text')
        else:
            return ''

    @staticmethod
    def Wrap(x):
        """
        Static method to wrap a dictionary recursively as an XmlDictObject
        """

        if isinstance(x, dict):
            return XmlDictObject((k, XmlDictObject.Wrap(v)) for (k, v) in x.iteritems())
        elif isinstance(x, list):
            return [XmlDictObject.Wrap(v) for v in x]
        else:
            return x

    @staticmethod
    def _UnWrap(x):
        if isinstance(x, dict):
            return dict((k, XmlDictObject._UnWrap(v)) for (k, v) in x.iteritems())
        elif isinstance(x, list):
            return [XmlDictObject._UnWrap(v) for v in x]
        else:
            return x
        
    def UnWrap(self):
        """
        Recursively converts an XmlDictObject to a standard dictionary and returns the result.
        """

        return XmlDictObject._UnWrap(self)

def _ConvertDictToXmlRecurse(parent, dictitem):
    assert type(dictitem) is not type([])

    if isinstance(dictitem, dict):
        for (tag, child) in dictitem.iteritems():
            if str(tag) == '_text':
                parent.text = str(child)
            elif type(child) is type([]):
                # iterate through the array and convert
                for listchild in child:
                    elem = ElementTree.Element(tag)
                    parent.append(elem)
                    _ConvertDictToXmlRecurse(elem, listchild)
            else:                
                elem = ElementTree.Element(tag)
                parent.append(elem)
                _ConvertDictToXmlRecurse(elem, child)
    else:
        parent.text = str(dictitem)
    
def ConvertDictToXml(xmldict):
    """
    Converts a dictionary to an XML ElementTree Element 
    """

    roottag = xmldict.keys()[0]
    root = ElementTree.Element(roottag)
    _ConvertDictToXmlRecurse(root, xmldict[roottag])
    return root

def _ConvertXmlToDictRecurse(node, dictclass):
    nodedict = dictclass()
    
    if len(node.items()) > 0:
        # if we have attributes, set them
        nodedict.update(dict(node.items()))
    
    for child in node:
        # recursively add the element's children
        newitem = _ConvertXmlToDictRecurse(child, dictclass)
        if nodedict.has_key(child.tag):
            # found duplicate tag, force a list
            if type(nodedict[child.tag]) is type([]):
                # append to existing list
                nodedict[child.tag].append(newitem)
            else:
                # convert to list
                nodedict[child.tag] = [nodedict[child.tag], newitem]
        else:
            # only one, directly set the dictionary
            nodedict[child.tag] = newitem

    if node.text is None: 
        text = ''
    else: 
        text = node.text.strip()
    
    if len(nodedict) > 0:            
        # if we have a dictionary add the text as a dictionary value (if there is any)
        if len(text) > 0:
            nodedict['_text'] = text
    else:
        # if we don't have child nodes or attributes, just set the text
        nodedict = text
        
    return nodedict
        
def ConvertXmlToDict(root, dictclass=XmlDictObject):
    """
    Converts an XML file or ElementTree Element to a dictionary
    """

    # If a string is passed in, try to open it as a file
    if type(root) == type(''):
        root = ElementTree.parse(root).getroot()
    elif not isinstance(root, ElementTree.Element):
        raise TypeError, 'Expected ElementTree.Element or file path string'

    return dictclass({root.tag: _ConvertXmlToDictRecurse(root, dictclass)})

def ReadTestFile(filename):
    configdict = ConvertXmlToDict(filename)
    standard_dict = configdict.UnWrap()
    return standard_dict
    
def PrintTestData(test_dict):
    test_step = test_dict["test"]    
    has_ob  = False
    if test_step.has_key("venue") :
        obs =test_step["venue"] 
        has_ob  = True
        if isinstance(obs, list):
            print "this step contains %d OB :"%(len(obs))
            for ob in obs :
                print "OB for venue_id :%s"%ob["id"]
                if ob.has_key("orderbook") :
                    print "OrderBook : %s"%ob["orderbook"]
        elif isinstance(obs, dict):
            print "OB for venue_id :%s"%obs["id"]
            if ob.has_key("orderbook") :
                print "OrderBook : %s"%ob["orderbook"]                            

def GetOrderList(test_dict, venue_name):
    test_step = test_dict["test"]    
    has_ob  = False
    Orderlist = list()
    
    if test_step.has_key("venue") :
        obs =test_step["venue"]
        has_ob  = True
        if isinstance(obs, list):
            print "this step contains %d OB :"%(len(obs))
            for ob in obs :
                if ob["id"] == venue_name :
                    print "OB found for %s"%venue_name
                    if ob.has_key("orderbook") :
                        MktDataEntries =ob["orderbook"]["MarketDataEntry"]
                        if isinstance(MktDataEntries, dict) :
                            size = MktDataEntries["Size"]
                            price =MktDataEntries["Price"]
                            side = MktDataEntries["Side"]
                            Orderlist.append({"price" : price, 'qty' : size, 'side' : side}) 
                            print "Order to send : p = %s, Q = %s, Side = %s"%(price, size,side)
                        elif isinstance(MktDataEntries, list) :
                            for m in MktDataEntries:
                                size = m["Size"]
                                price =m["Price"]
                                side = m["Side"]
                                Orderlist.append({"price" : price, 'qty' : size, 'side' : side})
                                print "Order to send : p = %s, Q = %s, Side = %s"%(price, size,side)
        elif isinstance(obs, dict):
            if obs["id"] == venue_name:
                print "OB found for %s"%venue_name
                if ob.has_key("orderbook") :
                    MktDataEntries =ob["MarketDataEntry"]
                    for m in MktDataEntries :
                        size = m["Size"]
                        price =m["Price"]
                        side = m["Side"]
                        print "Order to send : p = %s, Q = %s, Side = %s"%(price, size,side)                            
                        Orderlist.append({"price" : price, 'qty' : size, 'side' : side})
    return Orderlist

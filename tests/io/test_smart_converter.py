import lib.io.smart_converter as converter
from lib.io.fix import FixTranslator
from lib.io.serialize import *
import simplejson
from datetime import datetime
import types
import os

def test_converter():
    script_dir = os.path.dirname(__file__) + '/'
    
    file     = open(script_dir + 'orders.json', 'r')
    input    = file.read()
    file.close()
    
    dict_list= simplejson.loads(input, object_hook = as_datetime)
    
    file = open(script_dir + 'fix_types.json', 'r')
    input = file.read()
    file.close()

    list_map = simplejson.loads(input)["fix"]["fields"]["field"]
    
    file = open(script_dir + 'enrichment_types.json', 'r')
    input = file.read()
    file.close()
    
    list_map.extend(simplejson.loads(input))
    
    conv     = converter.Converter(list_map)
    
    for dict in dict_list:
        new_dict = {}
        for el in dict:
            new_dict[el] = conv.convert(el, dict[el])
            my_type = conv._map(el)
            assert isinstance(new_dict[el], my_type)
  
def test_fix_translator():
    ft = FixTranslator()
    
    assert ft.translate_tag(5) == "AdvTransType"
    assert ft.translate_tag('5') == "AdvTransType"
    assert ft.translate_tag("AdvTransType") == '5'
    
    s = "8= FIX.4.2|9=239|35=G|34=64|49=FLEX|50=ON1|52=20130806-06:37:58|56=ULPROD|57=T1|1=AKOCASH|11=AAA0022|21=1|37=2013080600000080|38=504|40=2|41=AAA0021|44=0.00000000|47=A|54=1|55=UCBb.AG|58=900|60=20130806-06:37:58|9000=BLCLNT1_4|9271=FY00010701342ESLO0CLNT1|10=000|"
    k = "8= FIX.4.2|9=239|35=G|34=64|49=FLEX|50=ON1|52=20130806-06:37:58|56=ULPROD|57=T1|1=AKOCASH|21=1|37=2013080600000080|38=504|40=2|41=AAA0021|44=0.00000000|47=A|54=1|55=UCBb.AG|58=900|60=20130806-06:37:58|9000=BLCLNT1_4|9271=FY00010701342ESLO0CLNT1|10=000|"
    
    test = """8=FIX.4.2|9=409|35=G|49=CLNT1|56=FLINKI|34=110|52=20130806-07:00:56|57=ON1|115=AKOCL|116=ALGO|1=AKOCASH|11=FY00010701549ESLO0|15=EUR|21=1|22=2|37=BFY00010701342ESLO0CLNT1|38=145000|40=2|41=FY00010701342ESLO0|44=48.88|48=5596991|54=1|55=UCBb.AG|59=0|60=20130806-07:00:56|100=BR|109=UK_OPD_OMAROUT|9251= |9250= |9252=3|9253=12|9254=20|9255= |9256= |9257=0|9258= |9259= |9260= |9261=no|9262= |9264= |9265=yes|9266=UCB.BR|9270= |10=009|
    8=FIX.4.2|9=319|35=8|34=152|49=FLINKI|50=ON1|52=20130806-07:00:56|56=CLNT1|128=AKOCL|129=ALGO|38=145000|6=0.00000000|11=FY00010701549ESLO0|14=0|15=EUR|17=1308068000000071|20=0|22=2|31=0.00000000|32=0|37=BFY00010701549ESLO0CLNT1|39=E|44=48.88000000|48=5596991|54=1|55=UCBb.AG|60=20130806-07:00:56|150=E|41=FY00010701342ESLO0|151=145000|10=076|
    8=FIX.4.2|9=292|35=8|34=164|49=FLINKI|50=ON1|52=20130806-07:02:54|56=CLNT1|128=AKOCL|129=ALGO|38=145000|6=0.00000000|11=FY00010701549ESLO0|14=0|15=EUR|17=1308068000000081|20=0|22=2|31=0.00000000|32=0|37=BFY00010701549ESLO0CLNT1|39=3|44=48.88000000|48=5596991|54=1|55=UCBb.AG|60=20130806-07:02:54|150=3|151=0|10=107|
    8=FIX.4.2|9=322|35=8|34=173|49=FLINKI|50=ON1|52=20130806-07:03:33|56=CLNT1|128=AKOCL|129=ALGO|38=145000|6=47.90000000|11=FY00010701549ESLO0|14=673|15=EUR|17=1308068000000090|20=0|22=2|31=0.00000000|32=0|37=BFY00010701549ESLO0CLNT1|39=5|44=48.88000000|48=5596991|54=1|55=UCBb.AG|60=20130806-07:03:33|150=5|41=FY00010701342ESLO0|151=144327|10=229|"""
     
    print ft.line_translator(s)
    print ft.pretty_print_jsonlike(s+'\n'+s, to_print=False)
    print ft.pretty_print_csv(s+'\n'+s, to_print=False)
    
    print ft.pretty_print_csv(s+'\n'+s, to_print=False, sep='\t')
    
    
    print ft.pretty_print_csv(s+'\n'+k, to_print=False, sep='\t')
    
    print ft.pretty_print_csv(test, to_print=False, sep='\t')
    import paramiko
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect('172.25.152.73', 
                username = 'flexsys', 
                password = 'flexsys1')
    cmd = 'prt_fxlog ./logs/trades/20130730/FLINKI_CLNT120130730I.fix 3' 
    
    (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
    s = stdout_grep.read()
    ssh.close()
    
    csv = ft.pretty_print_csv(s, to_print=False, sep=';')
    file = open("file.csv", "w")
    file.write(csv)
    file.close()
    
if __name__== "__main__":
    test_converter()
    test_fix_translator()
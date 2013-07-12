"""Important note: this script must be run from the project main directory"""

BASE_PATH = ""

def simep_start_init():
    # we import modules in the function avoid polluting the namespace
    import sys, os
    BASE_PATH = os.getcwd()
    run_unit_test = "--unittest" in sys.argv
    print "# Simep startup"

    #print "# - Adding simep package to python module search path"
    #sys.path.append(os.path.join(os.getcwd(), 'pysrc'))

    #print "# - Adding scripts folder to python module search path"
    #sys.path.append(os.path.join(os.getcwd(), 'scripts'))

    #print "# - Adding ./ (current working directory) to python module search path"
    #sys.path.append('./')
    
    #new_dir = 'C:\python\debug\Lib\site-packages\dev\tests'
    #print "# - Setting current directory to " + new_dir
    #sys.path.append(new_dir)
    os.chdir('C:\st_project\simep')

    # adding the FIX test folder to python module search path
    sys.path.append('C:/st_project/simep/FIX')

    if run_unit_test:
        print "# - Executing unit tests...\n\n"
        import run_all_tests
    else:
        autorun_path = os.path.join(BASE_PATH, 'scripts/autorun.py')
        if os.path.exists(autorun_path):
            print "# - Executing autorun python script"
            execfile(autorun_path, globals())
        else:
            print "# - (No autorun python script found)"
        print "#"

simep_start_init()

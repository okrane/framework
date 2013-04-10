# ------------------------------------------------- #
# Get Path                                          #
# ------------------------------------------------- #
import sys
import os.path
path = os.path.join(sys.argv[0].split('st_sim')[0],'st_sim')




# ------------------------------------------------- #
# Run GUI                                           #
# ------------------------------------------------- #

from simep.gui.mainwindow import MainWindow



MainWindow(InputXMLFile         = path + '/dev/tests/demo/st_sim_demo.xml',
           OutputFilesDirectory = path + '/dev/tests/demo/results',
           master               = None)
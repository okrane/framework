import os

# Update Windows path so it can access the SIMEP DLLs
_dll_dir = os.path.join(os.path.split(__file__)[0], 'dll')
os.environ['PATH'] += ';' + _dll_dir

def get_dll_dir():
    return _dll_dir
    
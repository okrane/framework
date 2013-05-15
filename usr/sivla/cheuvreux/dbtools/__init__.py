import os

is_pyodbc_available = False

# Check if the module pyodbc is available
try:
    import pyodbc
    is_pyodbc_available = True
except ImportError:
    # pyodbc is not available
    pass

# Try to figure out if we are in a production, homologation or dev environment
environment = 'production'

# We actually map the environment variable to the st_work connection mode
mode = os.getenv("MODE")
if mode == 'PROD':
    environment = 'production'
elif mode == 'HC':
    environment = 'homolo'
elif mode == 'DEV':
    environment = 'dev'

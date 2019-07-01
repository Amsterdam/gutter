import os

# HACK TO ACCESS gutterlib: set search path to main directory
os.sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gutterlib.apicentral.AccessController import AccessController

GUTTER_DATABASE = { 'db_type' : os.environ.get('GUTTER_DB_TYPE'),
                    'url' : os.environ.get('GUTTER_DB_URL'), 
                    'port' : os.environ.get('GUTTER_DB_PORT'), 
                    'user' : os.environ.get('GUTTER_DB_USER'), 
                    'password' : os.environ.get('GUTTER_DB_PASSWORD'), 
                    'name' : os.environ.get('GUTTER_DB_NAME') }

ac = AccessController()
ac.connect(**GUTTER_DATABASE)

# ==== SETTINGS =====

USER_NAME = "femke"
USER_PASSWORD = "amsterdam"

# ==== end settings ====

ac.create_user(email=USER_NAME, password=USER_PASSWORD)

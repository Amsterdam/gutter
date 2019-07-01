# runs flower tasks every X minutes

import os
import time

from gutterlib.flow.GutterFlow import GutterFlow
from gutterlib.datastore.GutterStore import GutterStore

import logging

""" SETTINGS
    
    Make sure these settings are set as environment variables 

"""

LOOP_SECONDS = 60*5 # loop every X seconds

GUTTER_DATABASE = { 'type' : os.environ.get('GUTTER_DB_TYPE'), 
                    'url' : os.environ.get('GUTTER_DB_URL'), 
                    'port' : os.environ.get('GUTTER_DB_PORT'), 
                    'user' : os.environ.get('GUTTER_DB_USER'), 
                    'password' : os.environ.get('GUTTER_DB_PASSWORD'), 
                    'name' : os.environ.get('GUTTER_DB_NAME') }

#### END SETTINGS ####

gutterFlow = GutterFlow()
gutterFlow.connect(**GUTTER_DATABASE)

gutterStore = GutterStore()
gutterStore.connect( **GUTTER_DATABASE )

gutterFlow.connectGutterStore( gutterStore )

print ('==== start python runner ====')

while True:
    
    try:
        l = "Run flow @{0}".format(time.ctime())
        print (l)
        
        # do the job
        try:
            r = gutterFlow.doJobs()
            if not r:
                print ('=> no jobs to do!') 
        
        except Exception as e:
            print (e)
        
        # go to sleep
        time.sleep( LOOP_SECONDS )
        

    except Exception as e:
        print (e)




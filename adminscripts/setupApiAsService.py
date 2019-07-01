"""

    setupNewPipelineAndApi.py

"""

import os

# HACK TO ACCESS gutterlib: set search path to main directory
os.sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gutterlib.flow.GutterFlow import GutterFlow
from gutterlib.datastore.GutterStore import GutterStore
from gutterlib.apicentral.ApiCentral import ApiCentral

# 0. settings

GUTTER_DATABASE = {'db_type': os.environ.get('GUTTER_DB_TYPE'),
                   'url': os.environ.get('GUTTER_DB_URL'),
                   'port': os.environ.get('GUTTER_DB_PORT'),
                   'user': os.environ.get('GUTTER_DB_USER'),
                   'password': os.environ.get('GUTTER_DB_PASSWORD'),
                   'name': os.environ.get('GUTTER_DB_NAME')}

SCHEMA = {
    "title": "waarnemingen",
    "properties": {
            "id":          {"type": "string"},
            "description": {"type": "string"},
            "some_number": {"type": "number"}
        }
}

# 1. create pipeline object

api_central = ApiCentral()
api_central.connect(**GUTTER_DATABASE)
api_central.create_api_as_service(schema_definition=SCHEMA)

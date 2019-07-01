# test ApiCentral

import os

# HACK TO ACCESS gutterlib: set search path to main directory
os.sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gutterlib.apicentral.ApiCentral import ApiCentral

GUTTER_DATABASE = {'db_type': os.environ.get('GUTTER_DB_TYPE'),
                   'url': os.environ.get('GUTTER_DB_URL'),
                   'port': os.environ.get('GUTTER_DB_PORT'),
                   'user': os.environ.get('GUTTER_DB_USER'),
                   'password': os.environ.get('GUTTER_DB_PASSWORD'),
                   'name': os.environ.get('GUTTER_DB_NAME')}

api_central = ApiCentral()
api_central.connect(**GUTTER_DATABASE)

# apiCentral.createIndicesOnEndpoint('sia')
api_central.create_indices_on_endpoint('schoongebieden_daystats')

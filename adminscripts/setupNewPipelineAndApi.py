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

PIPELINE_NAME = "sia"
GUTTER_DATABASE = {'db_type': os.environ.get('GUTTER_DB_TYPE'),
                   'url': os.environ.get('GUTTER_DB_URL'),
                   'port': os.environ.get('GUTTER_DB_PORT'),
                   'user': os.environ.get('GUTTER_DB_USER'),
                   'password': os.environ.get('GUTTER_DB_PASSWORD'),
                   'name': os.environ.get('GUTTER_DB_NAME')}

API_ENDPOINT_UNIT = "sia_melding"

# 1. create pipeline object

# TODO
# ( now done with pgadmin )

# 2. run flow for first time
'''
gutterFlow = GutterFlow()
gutterFlow.connect( **GUTTER_DATABASE )

gutterStore = GutterStore()
gutterStore.connect( **GUTTER_DATABASE )

gutterFlow.connectGutterStore( gutterStore )

pipelineObj = gutterFlow.executePipeline(pipeline=PIPELINE_NAME)

SCHEMA = pipelineObj.last_source_schema_def # get auto schema
'''

# SCHEMA = {"title": "vergun_evenementen_v2", "required": [], "properties": {"buurt": {"type": "string"}, "status": {"type": "string"}, "kenmerk": {"type": "string"}, "locatie": {"type": "string"}, "datum_tm": {"type": "string", "format": "date-time"}, "gebied22": {"type": "string"}, "latitude": {"type": "number"}, "tijd_tot": {"type": "string"}, "tijd_van": {"type": "string"}, "datum_van": {"type": "string", "format": "date-time"}, "longitude": {"type": "number"}, "resultaat": {"type": "string"}, "stadsdeel": {"type": "string"}, "straatnaam": {"type": "string"}, "werkzaamheden": {"type": "string"}, "thv_huisnummer": {"type": "string"}, "buurtcombinatie": {"type": "string"}, "soort_evenement": {"type": "string"}, "aantal_bezoekers": {"type": "number"}, "adwh_aanvraag_type": {"type": "string"}, "adwh_laatst_gezien": {"type": "string", "format": "date-time"}, "adwh_laatst_gezien_bron": {"type": "string", "format": "date-time"}}}
# SCHEMA = {"title": "sia", "properties": {"id": {"type": "integer"}, "text": {"type": "string"}, "image": {"type": "string"}, "_links": {"type": "object", "properties": {"self": {"type": "object", "properties": {"href": {"type": "string"}}}}}, "source": {"type": "string"}, "status": {"type": "object", "properties": {"id": {"type": "integer"}, "text": {"type": "string"}, "user": {"type": "string"}, "state": {"type": "string"}, "extern": {"type": "boolean"}, "target_api": {"type": "string"}, "extra_properties": {"type": "object", "properties": {"IP": {"type": "string"}}}}}, "_display": {"type": "string"}, "category": {"type": "object", "properties": {"sub": {"type": "string"}, "main": {"type": "string"}, "priority": {"type": "string"}, "department": {"type": "string"}}}, "location": {"type": "object", "properties": {"id": {"type": "integer"}, "address": {"type": "object", "properties": {"postcode": {"type": "string"}, "huisletter": {"type": "string"}, "huisnummer": {"type": "string"}, "woonplaats": {"type": "string"}, "openbare_ruimte": {"type": "string"}, "huisnummer_toevoeging": {"type": "string"}}}, "geometrie": {"type": "object", "properties": {"type": {"type": "string"}, "coordinates": {"type": "array"}}}, "stadsdeel": {"type": "string"}, "buurt_code": {"type": "string"}, "address_text": {"type": "string"}, "extra_properties": {"type": "string"}}}, "reporter": {"type": "object", "properties": {"email": {"type": "string"}, "phone": {"type": "string"}, "remove_at": {"type": "string", "format": "date-time"}, "created_at": {"type": "string", "format": "date-time"}, "updated_at": {"type": "string", "format": "date-time"}, "extra_properties": {"type": "string"}}}, "signal_id": {"type": "string"}, "created_at": {"type": "string", "format": "date-time"}, "text_extra": {"type": "string"}, "updated_at": {"type": "string", "format": "date-time"}, "extra_properties": {"type": "string"}, "operational_date": {"type": "string"}, "incident_date_end": {"type": "string"}, "incident_date_start": {"type": "string", "format": "date-time"}}}

# 3. create api endpoint
api_central = ApiCentral()
api_central.connect(**GUTTER_DATABASE)
api_central.create_api_as_service(schema_definition=SCHEMA, unit=API_ENDPOINT_UNIT)

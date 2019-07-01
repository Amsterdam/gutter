"""

    gutterlib.apicentral.ApiEndPoint
    
    Model for saving Gutter API endpoints

"""

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Boolean
from sqlalchemy.dialects.postgresql import JSONB

from flask_restplus import fields as restplus_fields

import json

DBObj = declarative_base()


class ApiEndPoint(DBObj):
    # basic model for saving all data rows

    __tablename__ = 'api_endpoints'
    __table_args__ = {"schema": "gutter"}

    name = Column(String(), primary_key=True)
    endpoint = Column(String())  # location endpoint name - ex: morameldingen
    unit = Column(String())  # name of the unit it supplies - ex: moramelding
    gutter_table = Column(String())  # name of gutter store table name
    schema_definition = Column(JSONB())
    active = Column(Boolean())
    anonymous_access = Column(Boolean())  # if we can access endpoint without tokens

    # ----

    def __init__(self, name=None, endpoint=None, unit=None, gutter_table=None,
                 schema_definition=None, active=None, anonymous_access=None):

        # NOTE: can be without parameters to only create table

        self.name = name
        self.endpoint = endpoint
        self.unit = unit
        self.gutter_table = gutter_table
        self.schema_definition = schema_definition
        self.active = active
        self.anonymous_access = anonymous_access

    # ----

    def __repr__(self):
        # string/unicode representation of object
        return "<ApiEndpoint name='{0}', endpoint='{1}', unit='{2}', " \
               "gutter_table='{3}', schema_definition='{4}', active='{5}', " \
               "anonymous_access='{6}'>".format(
                self.name,
                self.endpoint,
                self.unit,
                self.gutter_table,
                self.schema_definition,
                self.active,
                self.anonymous_access)

    # ----

    def to_dict(self):

        # TODO: make this more robust?
        allowed_properties_output = [str, bool, str, int, float, dict, list]

        # simple filter out internal sql alchemy keys
        d = {}

        for key in self.__dict__.keys():

            v = self.__dict__[key]

            if type(v) in allowed_properties_output:  # key[0] != '_': # first char not '_'
                d[key] = v
                

        return d

    # ----

    def to_json(self):
        return json.dumps(self.to_dict())
    
    # ----
    
    def to_restplus_marshall_fields(self):
        
        endpoint_marshall_model_fields = {}
        
        # NOTE: only simple ones
        PYTHON_TYPE_TO_MARSHALL_FIELD = { 'str' : restplus_fields.String, 
                                          'bool' : restplus_fields.Boolean,
                                          'int' : restplus_fields.Integer,
                                          'float' : restplus_fields.Float,
                                          'dict' : restplus_fields.Raw  # hack: see: https://flask_restplus.readthedocs.io/en/stable/_modules/flask_restplus/fields.html
                                         }
        
        for prop_name in self.__dict__.keys():
            
            marshall_field = PYTHON_TYPE_TO_MARSHALL_FIELD.get(str(type(prop_name)))
            
            if marshall_field:
                endpoint_marshall_model_fields[prop_name] = marshall_field(required=True) # set all fields required for now
        
        return endpoint_marshall_model_fields

    # ----

    def create_table(self, engine):

        if not engine:
            self.logger.error('create_table: Please supply engine')
            return False

        try:
            DBObj.metadata.create_all(engine)

        except Exception as e:
            print("ERROR: Can't create table for ApiCentral: {0}".format(unicode(e)))

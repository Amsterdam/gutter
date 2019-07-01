"""
    
    gutterlib.apicentral.RequestHandler
    
    Handles different request coming from API and get the data

"""

from ..datastore.GutterStore import GutterStore
from jsonschema import validate

import logging
import re
from psycopg2._json import json


class RequestHandler:

    # ----

    def __init__(self, api_central=None):

        # api_end_point = api_end_point
        self.api_central = api_central
        self.gutter_store = None

        self.logger = None
        self.setup_logger()

        # check
        if api_central is None:
            self.logger.error("Please supply api_central instance for this RequestHandler")
        
        # ---- debug ----
        self.logger.info("==== initialized RequestHandler ====")
        self.logger.info(self)
            

    # ----

    def __del__(self):

        pass

        return True

    # ----

    def connect(self, **kwargs):
        
        # NOTE: only use this when not sharing a central gutter store

        self.gutter_store = GutterStore()

        self.gutter_store.connect(**kwargs)
        
    # ----
    
    def connect_to_gutter_store(self, gutter_instance):
        
        self.gutter_store = gutter_instance
        
        # check
        if not self.gutter_store.is_connected:
            self.logger.warning("RequestHandler: connect_gutter_store: but this gutter store instance is not connected!")
        

    # ----

    def setup_logger(self):

        self.logger = logging.getLogger(__name__)

    # ==== managing data ====

    def check_gutter_store(self):

        is_good = True

        if self.gutter_store is None:
            is_good = False

        if is_good and not self.gutter_store.is_connected():
            is_good = False

        if not is_good:
            self.logger.error("RequestHandler is not well connected with GutterStore: no data!")
            return False

        return True

    # ----

    def get_data_by_id(self, api_end_point, id_):

        if not self.check_gutter_store():
            return False

        if not api_end_point:
            self.logger.error("Cannot get data without api_end_point")
            return False

        return self.gutter_store.get_data_by_id(
            table_name=api_end_point.gutter_table, id=id_)

    # ----

    def get_data_list(self, api_end_point, request_data):

        # request data: $filter, $select (disabled), $top, $skip, $order_by

        if not self.check_gutter_store():
            self.logger.error("No connection with GutterStore")
            return False

        if not api_end_point:
            self.logger.error("Cannot get data without api_end_point")
            return False

        # request is flask object containing: (response=None, status=None, headers=None, mimetype=None, content_type=None, direct_passthrough=False) 
        # see: http://flask.pocoo.org/docs/1.0/api/#flask.request

        # for now: use dictionary request_data
        # will contain $filter, $select, $top, $step

        # NOTE: only and filters
        # $filter: = id eq 'mora_1921821' and huisnummer gt 1000
        # prepare filters array with [{ 'column', 'logic', 'value'}] for gutter_store 
        filters = []
        if request_data.get('$filter') is not None:

            # security TODO: we can expect anything here
            # BUG with special chars ( fix in argparse )

            filter_str = request_data.get('$filter').replace('%20', ' ')  # .replace(' and ', ' ')

            # filter can be: 
            # without quotes: locatie.latitude gt 52.3 and locatie.latitude lt  52.4 
            # with quotes ( for filter values including spaces ): description eq 'hallo nog een test data entry!' and some_number eq 2001

            # note: using the and and $ to seperate filters makes quotes unneeded 
            # note: of course we still have a problem when there is a 'and' string in the filter value: solution: use different string; maybe $and ??? 

            filters_arr = re.findall('([^ ]+) (ge|gt|lt|eq|le|ne) \'?(.+?(?=and|$))\'?',
                                     filter_str)  # matches everything until either an 'and' or the end of the line

            for f in filters_arr:
                # value can be with quotes or without
                value = f[2]
                value = value.rstrip().lstrip()

                # just to show this case
                if "'" in value:
                    value = value.replace("'", "")

                filters.append({'column': f[0], 'logic': f[1], 'value': value})

        self.logger.info("Active filters for endpoint '/{0}': {1}".format(api_end_point.endpoint, filters))

        # pagination in $top and $skip
        try:
            top = int(request_data.get('$top'))
        except:
            top = None

        try:
            skip = int(request_data.get('$skip'))
        except:
            skip = None

            # $order_by=<colname>
        # for now only allow one column name ( and check for it )
        # can contain desc: $order_by=name desc
        order_by = request_data.get('$orderBy')
        order_by_type = None

        if order_by is not None:
            # NOTE: space to avoid ripping out parts of column name like in "description"
            if " desc" in order_by:
                order_by_type = "desc"
                # strip white space chars to be sure. note: no spaces in field names therefor
                order_by = order_by.replace("desc", "").rstrip().lstrip()
                
            if " asc" in order_by:
                order_by_type = "asc"
                order_by = order_by.replace("asc", "").rstrip().lstrip()

            if not self.schema_has_field_name(api_end_point, order_by):
                self.logger.error("Error in given orderBy field: {0}".format(order_by))
                order_by = None
                order_by_type = None

        # for special output like geojson
        format_ = request_data.get('$format')

        # request data from gutter store
        return self.gutter_store.get_data_list(
            table_name=api_end_point.gutter_table,
            schema_definition=api_end_point.schema_definition,
            select=None, filters=filters,
            limit=top, offset=skip,
            order_by=order_by, order_by_type=order_by_type,
            format=format_)

    # ----

    def insert_data(self, api_end_point=None, user=None, data=None):

        # POST to a URL creates a child resource at a server defined URL.
        # see: https://stackoverflow.com/questions/630453/put-vs-post-in-rest
        # return new row object

        if not self.check_gutter_store():
            return None

        if not api_end_point:
            self.logger.error("Cannot get data without api_end_point")
            return None

        if data is None:
            return None
        
        if type(data) == dict and len(data.keys()) == 0:
            return None

        # let's test if the incoming data corresponds to the schema
        api_end_point = self.fix_schema_required_field(api_end_point)
        validated = self.validate_data_with_json_schema(data, api_end_point.schema_definition)
        
        if not validated:
            self.logger.error("Cannot put data: does not fit into schema definition!")
            return None

        new_row = self.gutter_store.insert_data(table_name=api_end_point.gutter_table, user=user, data=data)

        return new_row

    # ----

    def update_data(self, api_end_point, data):

        # input is a json 
        # output is the updated row object

        if not self.check_gutter_store():
            return None

        # endpoint contains table_name and schema
        if not api_end_point:
            self.logger.error("Cannot get data without api_end_point")
            return None

        if data is None:
            return None
        if isinstance(data, dict) and len(data.keys()) == 0:
            self.logger.error("cannot update data: unknown input!")
            return None

        # let's test if the incoming data corresponds to the schema
        try:
            api_end_point = self.fix_schema_required_field(api_end_point)
            validate(data, api_end_point.schema_definition)
        except Exception as e:
            self.logger.error("Cannot put data: does not fit into schema definition! {0}".format(e))
            return None

        updated_row = self.gutter_store.update_data(
            table_name=api_end_point.gutter_table, data=data)

        return updated_row

    # ----

    def delete_data(self, api_end_point, id_):

        if not self.check_gutter_store():
            self.logger.error("Cannot delete data without a connection to GutterStore")
            return False

        r = self.gutter_store.delete_data(
            table_name=api_end_point.gutter_table, id=id_)

        # True or False
        return r

    # ==== utils ====

    def schema_has_field_name(self,  api_end_point, field_name):
        # NOTE: only on first level ( no nested fields )
        try:
            return field_name in api_end_point.schema_definition["properties"].keys()
        except Exception as e:
            self.logger.error(e)
            return False

    # ----
    
    def validate_data_with_json_schema(self, data, json_schema ):
        # add some extra stuff to allow null values
        required_columns = json_schema.get('required', [])
        if type(required_columns) == str: # can be string
            required_columns = [ required_columns ]
            
        # add allowed value null with type: basically add those to type definitions in array
        for property_name, property_def in json_schema['properties'].items():
            if property_name not in required_columns:
                if type(property_def['type']) is not list:
                    property_def['type'] = [ property_def['type'], 'null']
        try:
            validate(data, json_schema)
            return True
        except Exception as e:
            print (e)
            return False
        
        self.logger.info("Validated data: '{0}' <==> '{1}'".format(data, json_schema))

    
    # ----

    def fix_schema_required_field(self, api_end_point=None):
        
        """ Setup a pipeline
                
            the required field in a JSON schema can trip up the validation: correct this!
            
        """
        
        if api_end_point is None:
            self.logger.warning("fix_schema_required: no API end point given!")
            return None

        if type(api_end_point.schema_definition.get('required')) == list:
            if len(api_end_point.schema_definition['required']) == 0:
                # remove 'required' key otherwise trips up validation
                api_end_point.schema_definition.pop('required', None)
        if api_end_point.schema_definition.get('required') is None:
            api_end_point.schema_definition.pop('required', None)
            
        return api_end_point

""""

    GutterFlow.py

    * Handles ETL flows from foreign databases to the GutterStore

"""

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.inspection import inspect

import logging
import datetime
import re
import math

import copy
import simplejson as json

from .Pipeline import Pipeline
from .Database import Database
from .ApiSource import ApiSource

DBObj = declarative_base()


class GutterFlow:

    def __init__(self):

        # settings
        self.BATCHSIZE = 50

        # properties
        self.db_engine = None
        self.db_session_maker = None
        self.db_session = None

        self.connection_string = None
        self.logger = None
        self.has_connection = False
        self.gutter_store = None  # manager of gutter_store to push the data to
        self.api_source = None

        # setup
        self.setup_logger()

    # ----

    def __del__(self):
        # cleanup all connections
        if self.db_engine:
            self.db_engine.dispose()

    # ----

    def setup_logger(self):

        # NOTE: non-pep8 standard in logging library

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level=logging.INFO)

        try:
            handler = logging.StreamHandler()
            handler.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)-4s %(message)s')
            handler.setFormatter(formatter)
            
            if (self.logger.hasHandlers()): # see: https://stackoverflow.com/questions/7173033/duplicate-log-output-when-using-python-logging-module
                self.logger.handlers.clear()
            
            self.logger.addHandler(handler)

        except Exception as e:
            self.logger.error(e)

    # ----

    def connect(self, db_type, url, port, user, password, name):

        """ Connect to a specific database
            
        """

        try:
            self.connection_string = '{0}://{1}:{2}@{3}:{4}/{5}'.format(db_type, user, password, url, port, name)

            self.db_engine = create_engine(self.connection_string, echo=False,
                                           pool_pre_ping=True)  # for pool_pre_ping see: https://docs.sqlalchemy.org/en/latest/core/pooling.html
            self.db_session_maker = sessionmaker()
            self.db_session_maker.configure(bind=self.db_engine)
            self.db_session = self.db_session_maker()
            self.db_session.autoflush = True

            # test database connection
            self.db_engine.connect()  # db_engine is lazy, does not connect directly only if we do so

            # if connected 
            self.has_connection = True  # if connection is succesful this flag is set, otherwise Exception will happen
            self.logger.info("Succesfully connected to Gutter database")

            return True

        except Exception as e:
            self.logger.error("GutterFlow: Cannot connect to Gutter database! \n{0}".format(e))
            self.has_connection = False

            return False

    # ----

    def connect_gutter_store(self, gutter_store):

        self.gutter_store = gutter_store

        self.logger.info('Connected this GutterFlow to a GutterStore!')

        return True

    # ----

    def create_pipeline(self, name, source, map=None):

        """ Setup a pipeline
        
        :param name: Name of the pipeline
        :type name: str
        :param source: Source definition { url, name, port, type, user, table, schema, password }
        :param map: way to map incoming data to target schema { inp_column : output_column } or { inp_column : 'F(input)' }
        
        """

        if not self.has_connection:
            self.logger.error("Cannot create pipeline object without database connection!")
            return False

        try:
            new_pipeline = Pipeline(name, source, map)

            self.db_session.add(new_pipeline)
            self.db_session.commit()

            return new_pipeline

        except Exception as e:

            self.logger.error("error creating pipeline: {0}".format(e))

            return None

    # ----

    def get_pipeline(self, name):

        # get last by name
        pipeline = self.db_session.query(Pipeline).filter(Pipeline.name == name).first()

        return pipeline

    # ----

    def get_pipelines(self):

        try:
            pipelines = self.db_session.query(Pipeline).all()
        except Exception as e:
            self.logger.error("Error getting pipelines: {0}".format(e))
            return []

        return pipelines

    # ----

    def do_jobs(self):

        self.check_pipelines_time_out()

        # check for pipelines to do
        pipelines = self.get_pipelines()

        if len(pipelines) == 0:
            self.logger.error("No pipelines to do! do you have pipelines defined in the database?")
            # reset session too avoid open connections
            return False

        pipeline_names = ', '.join([pipeline.name for pipeline in pipelines])
        self.logger.info(
            "do_jobs: check if we need to run any of the {0} pipelines: '{1}'".format(len(pipelines), pipeline_names))

        for pipeline in pipelines:
            if pipeline.needs_doing():
                self.logger.info("Start pipeline {0}".format(pipeline.name))
                self.execute_pipeline(pipeline)

        if not True in [pipeline.needs_doing() for p in pipelines]:
            self.logger.info("No pipelines that needed execution!")

        return True

    # ----

    def check_pipelines_time_out(self):

        """ Check for pipelines that take too long and set flag to not executing
        
            TODO: these way of working needs to be replaced with a good queue system
        
        """

        pipelines = self.get_pipelines()

        DEFAULT_TIMEOUT = 60 * 10  # 10 minutes

        for pipeline in pipelines:
            if pipeline.executing:
                execution_duration = (datetime.datetime.now() - pipeline.last_run).total_seconds()
                time_out_duration = pipeline.max_duration or default_timeout

                if execution_duration > time_out_duration:
                    pipeline.executing = False  # cancel execution state to fix broken pipelines

        self.update_pipelines()

    # ----

    def execute_pipeline_by_name(self, name):

        if name is None:
            self.logger.error("Execute_pipeline_by_name: no name given!")
            return False

        pipeline = self.get_pipeline(name)

        return self.execute_pipeline(pipeline)

    # ----

    def execute_all_pipelines(self):

        pipelines = self.get_pipelines()

        for pipeline in pipelines:
            try:
                self.execute_pipeline(pipeline)
            except Exception as e:
                self.logger.error("Error executing pipeline: {0}".format(e))

    # ----

    def execute_pipeline(self, pipeline=None):

        """ Execute a given pipeline object
        
        :param pipeline: a Gutter Pipeline instance or pipeline name
        :return: bool -- Success or Fail
        
        """

        start_time = datetime.datetime.now()

        # test for connections
        if self.gutter_store is None:
            self.logger.error("Execute pipeline: You need to connect this GutterFlow to a GutterStore instance!")
            return False

        if not self.gutter_store.is_connected():
            self.logger.error("Execute pipeline: You need to connect with your GutterStore instance")
            return False

        # step 1: INPUT - check pipeline instance
        if pipeline is None:
            self.logger.error('Please supply a pipeline instance to execute!')
            return False

        if type(pipeline) is str:
            # execute this specific wrapper for execute_pipeline
            return self.execute_pipeline_by_name(
                pipeline)  # this will basically get pipeline object by name and return to this function

        self.logger.info("==== Start execution of pipeline job: '{0}' ====".format(pipeline.name))

        # step 2: SOURCE - get source model
        if pipeline.type == 'database' or pipeline.type is None:  # this is the default
            self.get_pipeline_source_schema_and_model(
                pipeline)  # data_source: dict with { type, url, user, port, password, schema, table }
        else:
            # API pipeline
            self.api_source = ApiSource(pipeline.data_source)
            pipeline.source_schema_definition = self.api_source.get_schema_definition(title=pipeline.name)

        # check for source_schema_definition
        if pipeline.source_schema_definition is None:
            self.logger.error("Cannot execute pipeline '{0}': "
                              "no source model. aborting. check existence of supplied table'")
            return False

        # step 3: SCHEMA CHECK - check if schema definition is altered: if yes take action
        self.check_pipeline_for_schema_definition_change(pipeline)

        # step 4: MAP - get or create source to target map
        self.setup_source_to_target_map(pipeline)
        self.update_pipelines()  # save changes to pipeline NOTE: needed?

        # step 5: STORAGE ROW MODEL - gutter storage row model ( referring for table belonging to schema definition )
        table_name = pipeline.source_schema_definition['title']
        # NOTE: these are classes for the models
        StorageModel = self.get_storage_model_from_gutter_store(table_name=table_name)
        HistoryModel = self.get_history_model_from_gutter_store(table_name=table_name)

        # step 6: CHECKS
        if StorageModel is None:
            pipeline.executing = False
            self.update_pipelines()
            self.logger.error("No working storage model. Aborting pipeline execution! "
                              "Please connect GutterStore instance")
            return None

        # make sure it is flagged as being executed
        pipeline.last_run = datetime.datetime.now()
        pipeline.executing = True
        self.update_pipelines()

        # step 7: transfer data
        results = self.transfer_data(pipeline, StorageModel, HistoryModel)  # results is a dict : { new , updates }

        # step 8: finish
        pipeline.executing = False
        pipeline.last_duration = (datetime.datetime.now() - start_time).total_seconds()
        self.update_pipelines()

        # step 9: check results and output

        if results is False:
            self.logger.info("==== Pipeline job '{0}' failed. See ERROR above ====")
            return False
        else:
            self.logger.info(
                "==== Pipeline job '{0}' successful with {1} new and {2} updates and the same {3} ( took: {4}s ) ====".format(
                    pipeline.name, results.get('new'), results.get('updates'), results.get('same'),
                    pipeline.last_duration))
            return True

    # ----

    def get_pipeline_source_schema_and_model(self, pipeline):

        """ Get source and model class for given pipeline
            
            NOTE: we do schema and model together to avoid two queries to the database
        
        :param pipeline: Instance of Gutter Pipeline
        :return: (dict,Model) -- 
        
        """

        if pipeline is None:
            self.logger.error("get_pipeline_source_schema_and_model: no pipeline given!")
            return None, None

        data_source_obj = pipeline.data_source  # { type, url, port, user, pasw, name }

        if not data_source_obj:
            self.logger.error('get_pipeline_source_schema_and_model: please give a data_source dict')
            return False

        source_database = self.get_database(data_source_obj)
        pipeline.source_table = source_database.get_table(schema=data_source_obj.get('schema'),
                                                          name=data_source_obj.get('table'))

        # save on pipeline instance
        pipeline.source_schema_definition = pipeline.source_table.get_schema_definition()  # json schema definition of data
        pipeline.source_model = pipeline.source_table.get_model(manual_primary_key=pipeline.primary_key)

        # return both schema and model class
        return pipeline.source_schema_definition, pipeline.source_model

    # ----

    def check_pipeline_for_schema_definition_change(self, pipeline):

        """ For now only detects a change, does not do anything 
        
        """

        if pipeline.source_schema_definition is None:
            self.logger.warn("Could not check schema_definition change of pipeline: no source_schem_definition!")

        if pipeline.last_source_schema_def != pipeline.source_schema_definition:
            # new or altered schema definition
            pipeline.last_source_schema_def = pipeline.source_schema_definition  # save last source definition
            # TODO: take action on change
            return True
        else:
            self.logger.info('no schema definition change!')
            return False

    # ----

    def setup_source_to_target_map(self, pipeline):

        """ a Flow can map input to output and with this transform data
        
        :return: dict or None -- used input-to-output map
        
        """

        self.logger.info("setup_source_to_target_map")

        # either the pipeline contains a map, or a map is automatically generated
        if pipeline.source_schema_definition is None:
            self.logger.warn("could not check schema_definition change of pipeline: no source_schem_definition!")
            return None

        pipeline.map = pipeline.map_source_target  # map given by user

        if not self.is_valid_source_target_map(pipeline.map):
            pipeline.map = self.create_auto_source_target_map(pipeline.source_schema_definition)
            pipeline.last_automap = pipeline.map  # save auto map in pipeline object

            self.logger.info('No given source to target map: created auto map: {0}'.format(pipeline.map))

            return pipeline.map

            # ----

    def update_pipelines(self):

        # saves pipeline objects back to database
        self.db_session.commit()

    # ----

    def get_storage_model_from_gutter_store(self, table_name):

        self.logger.info("get_storage_model_from_gutter_store")

        # get storage model from gutter_store 
        if self.gutter_store is None:
            self.logger.error(
                "get_storage_model: you need to connect this GutterFlow instance to a GutterStore instance!")
            return None

        if not self.gutter_store.is_connected():
            self.logger.error("Cannot save to a unconnected gutter_store!")
            return None

        storage_model = self.gutter_store.get_storage_model(table_name)

        # make sure we have a real table for this model
        if storage_model:
            storage_model().create_table(engine=self.db_engine)  # create table if not exists

        return storage_model

    # ----

    def get_history_model_from_gutter_store(self, table_name):

        # get storage model from gutter_store 
        self.logger.info("get_history_model_from_gutter_store")

        if self.gutter_store is None:
            self.logger.error("get_storage_model: you need to connect this GutterFlow to a GutterStore instance!")
            return None

        if not self.gutter_store.is_connected():
            self.logger.error("Cannot save to a unconnected GutterStore!")
            return None

        history_model = self.gutter_store.get_history_model(table_name)

        # make sure we have a real table for this model
        if history_model:
            history_model().create_table(engine=self.db_engine)  # create table if not exists

        return history_model

    # ----

    def transfer_data(self, pipeline, StorageModel, HistoryModel=None):

        """ Finally transfers the data from a outside source to Gutter in batches
            
            NOTES:
            - we use GutterStore to handle all storages ( don't mix database sessions )
            - we query source data through the pipeline.source_table GutterFlow Table instance
        
        :return False ( fail ) or result stats dict { updates : integer, new : integer , same : integer }  
        
        """

        self.logger.info("Start transfer of data")

        # some checks
        if pipeline is None:
            self.logger.error('Transfer data failed: no pipeline defined!')
            return False
        if pipeline.type == 'database' and (
                pipeline.source_table is None or pipeline.source_model is None or pipeline.source_schema_definition is None):
            self.logger.error(
                'Transfer data failed: no info on source data: check source_model and source_schema_definition!')
            return False

        if pipeline.type == 'api':
            pass  # TODO checks for api pipeline
        if StorageModel is None:
            self.logger.error('Transfer data failed: no StorageModel to write to!')
            return False

        primary_key_name = pipeline.primary_key

        if primary_key_name is None:
            primary_key_name = self.get_model_primary_key(pipeline.source_model)

        if primary_key_name is None:
            self.logger.error("Cannot transfer data without a known primary key name!")
            return False

        self.logger.info("Transfer data with primary_key_name : '{0}'".format(primary_key_name))

        # start batch rows
        batch_num = 0

        if pipeline.type == 'database':
            # NOTE: table instance maintains its own database session of the source database
            # NOTE: in the start_query() function we use order_by(Model.id) to ensure we get all the data ordered by id
            source_rows_in_batch = pipeline.source_table.start_query().offset(0).limit(self.BATCHSIZE).all()
        elif pipeline.type == 'api':
            if self.api_source is None:  # make sure it is here
                self.api_source = ApiSource(pipeline.data_source)
            source_rows_in_batch = self.api_source.get_batch_rows(batch_num)

        # main transfer loop
        num_new_rows = 0
        num_updated_rows = 0
        num_same_rows = 0

        while len(source_rows_in_batch) != 0:

            # try:
            sync_table = {}  # table with source rows and storage rows by primary key

            for obj in source_rows_in_batch:
                # object can be a SourceRow object ( from database ) or a dict from API

                if isinstance(obj, dict):
                    id = obj.get(primary_key_name)
                else:
                    id = getattr(obj, primary_key_name)

                sync_table[str(id)] = {'id': str(id), 'source': obj,
                                       'storage': None}  # synctable by id, with source row object and storage row object

            # find existing storage rows with incoming ids ( to update later )
            storage_rows = self.gutter_store.get_model_rows_by_ids(model=StorageModel, ids=sync_table.keys())

            for existing_row in storage_rows:
                sync_table[str(existing_row.id)]['storage'] = existing_row

            # now do sync: insert or check/update
            new_rows = []
            new_history_rows = []
            updated_rows = []
            same_rows = []

            # iterate over primary keys if incoming source rows
            for id in sync_table.keys():

                sync_row = sync_table[id]
                sync_source_row = sync_row['source']  # can be source_row object or dict from api
                sync_storage_row = sync_row['storage']

                now = datetime.datetime.now()

                # map data
                mapped_data = self.map_data(sync_source_row, sync_storage_row,
                                            pipeline.map)  # maps source data to storage data, the map can contain python functions

                if sync_storage_row is None:  # new row
                    # self.logger.warning('no existing row found for id {0}, create new'.format(id))

                    # create new storage row
                    new_storage_row = StorageModel(id=id,
                                                   created_at=now,
                                                   last_checked=now,
                                                   last_updated=now,
                                                   pipeline_id=pipeline.id,
                                                   data=mapped_data)

                    new_rows.append(new_storage_row)

                else:
                    # update row

                    # DEBUG row data comparison
                    # self.logger.info("existing row: {0} <===> new row: {1}".format(sync_storage_row.data, mapped_data))

                    if sync_storage_row.data != mapped_data:

                        if HistoryModel:
                            # save old data in history row
                            sync_history_row = HistoryModel(row_id=sync_storage_row.id, pipeline_id=pipeline.id)
                            sync_history_row.data = copy.deepcopy(sync_storage_row.data)
                            sync_history_row.valid_from = sync_storage_row.last_updated
                            sync_history_row.valid_to = now
                            new_history_rows.append(sync_history_row)

                            sync_storage_row.data = {}
                            sync_storage_row.data = mapped_data
                            sync_storage_row.last_checked = datetime.datetime.now()
                            sync_storage_row.last_updated = datetime.datetime.now()

                            updated_rows.append(sync_storage_row)

                    else:
                        sync_storage_row.last_checked = now
                        same_rows.append(sync_storage_row)

                        # batch end
            if len(new_rows) > 0:
                self.gutter_store.add_rows(new_rows)
                num_new_rows += len(new_rows)
            if len(new_history_rows) > 0:
                self.gutter_store.add_rows(new_history_rows)
                num_updated_rows += len(updated_rows)

            num_same_rows += len(same_rows)

            self.gutter_store.commit()  # make update

            # debug
            self.logger.info('==> batch {0} with {1} inserts, '
                             '{2} updates and {3} remained the same'.format(
                batch_num,
                len(new_rows),
                len(updated_rows),
                len(same_rows)))

            batch_num += 1

            # get new batchget_model
            if pipeline.type == 'database':
                source_rows_in_batch = pipeline.source_table.start_query().offset(batch_num * self.BATCHSIZE).limit(
                    self.BATCHSIZE).all()  # important: don't use self.db_session since that is gutter db
            elif pipeline.type == 'api':
                source_rows_in_batch = self.api_source.get_batch_rows(batch_num)

        # something terrible executing this batch
        # except Exception as e:
        #    self.logger.error("Failed batch: {0}".format(e))
        #    return False

        # end while loop and return total results
        return {'updates': num_updated_rows, 'new': num_new_rows, 'same': num_same_rows}

        # ----

    def map_data(self, source_obj, storage_obj, map):

        """
            Map a source row to a storage row

            :param source_obj: Can be a ORM Row instance or dict ( from API )
            :param storage_obj: ORM GutterRow instance
            :return: dict -- the data in key,value pair dict
        
        """

        json_data = {}  # on storage row object

        # maps the data from source object to storage object
        for output_property, map_value in map.items():
            # map value can be a simple name of input property or can contain python logic
            # Complex case: name : lower(name_input) or name : surname + ' ' + family_name 

            # TODO: check if output property ( is defined in target_schema_def of pipeline object

            output = None

            if map_value is None:
                self.logger.warning(
                    "No map value for output '{0}': Please check the given map! We default to None".format(
                        output_property))
                output = None
            # simple mapping 'column name' : 'same_column name'
            elif output_property == map_value:

                if type(source_obj) is dict:  # for data from API
                    output = source_obj.get(map_value)
                else:
                    # DataRow input
                    incoming_value = getattr(source_obj, map_value)

                    output = incoming_value
            # complex value expression
            else:
                output = self.eval_map_expression(source_obj, map_value)  # map_value is an expression

            # We are done for this property: output value is in output
            # IMPORTANT: FILTER - Make sure value types like decimals fit in JSON
            try:
                output = json.loads(json.dumps(output,
                                               default=str))  # default is just a fall_back and serializes everything to a string
            except Exception as e:
                self.logger.error("Error serializing property {0}={1}".format(output_property, output))
                self.logger.error(e)

            json_data[output_property] = output

        return json_data

    # ----

    def eval_map_expression(self, source_obj, map_expression):

        """
            We allow for python code to run as part of the mapping process ( for example to enrich the data )
        
            NOTES: 
                - expressions are strings from defined datamap object in database
                - expressions use "input" as reference to incoming data_row or data_dict 
                - For example: input.<<input column>> 
                    or we can use python logic:  input['col1'] + input['col2'], or access subfields: input['col1']['key']
                -  afterwards it is evaluated as python object code with eval
            WARNING: Don't use " in expression because it messes up the JSON
            
            :param source_obj: SourceRow instance or dict with data
            :param map_expression: the complex expression mapping the input to output
            :return: data value or None -- Can be numeric or string 
        
        """

        if map_expression is None:
            self.logger.error("Empty expression in map")
            return None

        # NOTE: source_obj can be source_row or dict ( dict is coming from api )

        # set up the input as a dict
        if type(source_obj) is dict:
            prop_names = source_obj.keys()
            prop_values = source_obj.values()
        else:
            prop_names = source_obj.get_props()  # NOTE: this can be unstable when there is a unknown data type in source
            prop_values = [getattr(source_obj, p) for p in prop_names]  # all strings

        input = dict(zip(prop_names, prop_values))

        try:
            output = eval(map_expression)  # !!!! TODO: evaluate security !!!!
            self.logger.info(
                "Succesfully evaluated map_expression: '{0}' to value '{1}'".format(map_expression, output))
            return output
        except Exception as e:
            self.logger.error("failed to evaluate map_expression: {0}. Output is set to None!".format(map_expression))
            return None

    # ----

    def create_auto_source_target_map(self, source_schema_def):

        # make map that is 1:1
        auto_map = {}

        for property_name, property_obj in source_schema_def.get('properties').items():
            auto_map[property_name] = property_name

        return auto_map

    # ----

    def is_valid_source_target_map(self, map_obj):

        return not (map_obj == 'null' or map_obj is None or map_obj == {})

        # ----

    def debug_test_write(self):

        if self.gutter_store is None:
            self.logger.error("no gutterstore!")
            return False

        StorageModel = self.gutter_store.get_storage_model(table_name="gebieden_noord_view")
        test_row = self.db_session.query(StorageModel).filter(StorageModel.id == '5').first()

        test_row.data = {'changed': 'yes'}
        self.db_session.commit()

    # ----

    def debug_test_write_lookup(self):

        if self.gutter_store is None:
            self.logger.error("no gutterstore!")
            return False

        StorageModel = self.gutter_store.get_storage_model(table_name="gebieden_noord_view")
        test_rows = self.db_session.query(StorageModel).limit(10).all()

        lookup = {}
        for r in test_rows:
            lookup[unicode(r.id)] = {'storage': r}

        # change
        for k, d in lookup.items():
            d['storage'].data = {'random': 'data2'}

        self.db_session.commit()

    # ==== utils ====

    def overlaps_with_other_strings(self, s, l):

        for string in l:
            if s in string and string != s:
                return True

        return False

    # ----

    def clean_values(self, v):

        if v is None:
            return 'None'
        # value can also be a dictionary
        if type(v) is dict:
            for s in v.values():
                self.clean_values(s)  # recursively clean strings
        elif type(v) is str:
            # general cleaning of database values
            v = v.replace('/', '\/')
            v = v.replace('\\', '')
            v = v.replace('"', "'")  # avoid errors when " in data
            v = v.rstrip()
            v = v.lstrip()

        return v

    # ----

    def string_is_int(self, s):

        try:
            int(s)
            return True
        except value_error:
            return False

    # ----

    def string_is_float(self, s):

        try:
            float(s)
            return True
        except value_error:
            return False

    # ----

    def string_is_number(self, s):

        return self.string_is_float(s)

    # ----

    def convert_string_to_orig_type(self, s):

        if s is None:
            return None
        elif s == '':
            return None
        elif self.string_is_int(s):
            if math.isinf(int(s)):
                return None
            else:
                return int(s)
        elif self.string_is_float(s):
            if math.isinf(float(s)):
                return None
            else:
                return float(s)
        else:
            return s

    # ----

    def get_database(self, source_dict):

        """ Get GutterFlow Database instance given the source dict
        
        :param source_dict: { name, url, password, port, db_type }
        :return: Database -- Gutter Flow Database instance
        
        """

        if not source_dict.get('name') or not source_dict.get('url') or not source_dict.get(
                'password') or not source_dict.get('password') or not source_dict.get('port') or not source_dict.get(
            'db_type'):
            self.logger.error(
                "Cannot get source database: insufficient input. Supply a dict with { name, url, password, port, type }")
            return False

        database = Database(db_type=source_dict.get('db_type'),
                            url=source_dict.get('url'),
                            port=source_dict.get('port'),
                            name=source_dict.get('name'),
                            user=source_dict.get('user'),
                            password=source_dict.get('password'))

        return database

    # ----

    def get_model_primary_key(self, Model):

        # input is (dynamic) sql_alchemy orm model class
        try:
            return inspect(Model).primary_key[0].name
        except Exception as e:
            # guess by name of property
            primary_key_name = self.get_model_primary_key_guess(Model)

            if primary_key_name is None:
                self.logger.warn(
                    "ERROR: GutterFlow cannot detect primary key for table '{0}': etl is probably faulty!".format(
                        model.__table__))
                return None
            else:
                return primary_key_name

    # ----

    def get_model_primary_key_guess(self, Model):
        # input is (dynamic) sql_alchemy orm model class
        # just iterate over properties and based on name deside on primary_key_name ( only when inspection fails)

        PROBABLY_ID_COLUMN_NAMES = ["id", "^id[^$]+?", "$[a_za_z_]+id^"]

        prop_names = [p for p in dir(Model) if p[0] != '_']

        candidates = []

        for pattern in PROBABLY_ID_COLUMN_NAMES:
            for prop_name in prop_names:
                matches = re.findall(pattern, prop_name)
                for m in matches:
                    if m not in candidates:
                        candidates.append(m)

        if len(candidates) > 0:
            return candidates[0]  # pick first canditate
        else:
            return None

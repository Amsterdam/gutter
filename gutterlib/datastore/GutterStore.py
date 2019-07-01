"""

    datastore.gutter_store.py
    
    * stores data in flexible way based on json capacity of postgres
    * handles metadata: logs updates
    

"""

from .GutterStoreError import GutterStoreError

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Numeric, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql.expression import cast
from sqlalchemy import desc
from sqlalchemy import func

import operator
import logging
import datetime
import re
import uuid
import simplejson as json

import geojson
import shapely.wkt
from functools import partial
import pyproj
from shapely.ops import transform

DBObj = declarative_base()


class GutterStore:

    def __init__(self):

        # settings
        self.GET_NUM_ROWS_DEFAULT = 2000
        self.GET_MAX_ROWS = 10000

        # properties
        self.db_engine = None
        self.db_session_maker = None
        self.db_session = None

        self.connection_data = {}
        self.connection_string = None
        self.logger = None
        self.has_connection = False

        self.storage_models_cache = {}  # key by table name
        self.storage_history_models_cache = {}

        # setup
        self.setup_logger()

    # ----

    def __del__(self):
        # cleanup all connections
        if self.db_engine:
            try:
                self.db_engine.dispose()
            except Exception as e:
                # KeyError: (<weakref at 0x041358A0; to 'function' at 0x0486BF18 (on_connect)>,)
                # TODO: check
                pass

    # ----

    def setup_logger(self):

        self.logger = logging.getLogger(__name__)

    # ----

    def connect(self, db_type, url, port, user, password, name):
        
        # connect to specific database
        if self.has_connection:
            self.logger.info("Gutter Store already got a connection!")
            return True

        try:
            self.connection_string = '{0}://{1}:{2}@{3}:{4}/{5}'.format(
                db_type, user, password,
                url, port, name)
            self.connection_data = {
                'db_type': db_type, 'url': url, 'port': port,
                'user': user, 'password': password, 'name': name
            }

            self.db_engine = create_engine(self.connection_string, echo=False)
            self.db_session_maker = sessionmaker()
            self.db_session_maker.configure(bind=self.db_engine)
            self.db_session = self.db_session_maker()

            # test database connection
            self.db_engine.connect()  # db_engine is lazy, does not connect directly only if we do so
            self.has_connection = True  # if connection is succesful this flag is set, otherwise Exception will happen
            self.logger.info("GutterStore instance connected to database. Given parameters: type='{0}', url='{1}', port='{2}', name='{3}'"
                             .format(self.connection_data['db_type'], self.connection_data['url'], self.connection_data['port'], self.connection_data['name']))

            return True

        except Exception as e:
            self.logger.error("Cannot connect to Gutter database! \n{0}".format(e))

            return False
        
    # ----
    
    def is_connected(self):
        
        return self.has_connection

    # ----

    def get_storage_model(self, table_name):

        if self.storage_models_cache.get(table_name) is not None:
            return self.storage_models_cache.get(table_name)

        # get a model for a row in which gutter saves all data

        TABLE_PRE_STRING = ""  # can be "gutter_" for example
        TABLE_POST_STRING = ""  # can be "_gutter" for example

        # ==== dynamic StorageRow class ====

        class StorageRow(DBObj):

            DBObj.metadata.clear()
            # basic model for saving all data rows

            __tablename__ = TABLE_PRE_STRING + table_name + TABLE_POST_STRING  # to extend
            __table_args__ = {"schema": "gutter_data"}  # hack to specify schema

            id = Column(String(), primary_key=True)  # string
            created_by = Column(String())
            created_at = Column(DateTime)
            last_checked = Column(DateTime)
            last_updated = Column(DateTime)
            pipeline_id = Column(Integer())
            data = Column(JSONB())

            # ----

            def __init__(self, id=None, created_by=None, created_at=None, last_checked=None, last_updated=None, pipeline_id=None, data=None, datahash=None):

                """ Init a storage ORM storage row
                
                NOTES on meta-properties:
                
                Values for meta-properties can be submitted in a JSON payload and need to be translated to internal Gutter ones.
                They are:
            
                * id - default is uuid
                * created_at - default is now
                
                due to security reasons 'created_by' cannot be set by user
                
                After we received these meta properties they are brought to the gutter row columns ( id, created_by, created_at ) and are removed from the data. 
                When outputting a data row the meta-properties are brought back into the data in format  _id, _created_at, _created_by   
            
                """
                
                # NOTE: can be without parameters to only create table
                
                # clean data from meta-properties
                if type(data) is dict:
                    data.pop('id', None)
                    data.pop('created_at', None)
                
                # default value for id when not given
                if id is None:
                    # NOTE: for now we only use string unique ids _ either from source or random uuid
                    id = uuid.uuid4()  # a unique string of numbers

                self.id = str(id)
                self.created_by = created_by
                self.created_at = created_at or datetime.datetime.now()
                self.last_checked = last_updated or datetime.datetime.now()
                self.last_updated = last_updated or datetime.datetime.now()
                self.pipeline_id = pipeline_id
                self.data = data
                self.datahash = datahash


            # ----

            def __repr__(self):

                return "<GutterRow table_name='{0}', id='{1}', created_by='{2}' created_at='{3}', \
                            last_updated='{4}', checked_last='{5}', pipeline_id='{6}', \
                            data='{7}'>".format(self.__tablename__, self.id, self.created_by, self.created_at, self.last_updated,
                                                self.last_checked, self.pipeline_id, self.data)

            # ----

            def to_dict(self):

                ALLOWED_PROPERTIES_OUTPUT = [str, bool, str, int, float, long, dict, list]

                # simple filter out internal sql alchemy keys
                d = {}

                for key in self.__dict__.keys():

                    v = self.__dict__[key]

                    if type(v) in ALLOWED_PROPERTIES_OUTPUT:  # key[0] != '_': # first char not '_'
                        if (type(v) is str) and v is not None:
                            d[key] = v.encode('utf8', errors='ignore')  # force utf_8 encoding
                        else:
                            d[key] = v

                return d

            # ----

            def get_data_dict(self):

                # add id with it
                d = self.data
                d['_id'] = self.id
                d['_created_at'] = self.created_at.isoformat()
                d['_created_by'] = self.created_by

                return d

            # ----

            def to_json(self):
                return json.dumps(self.to_dict())

            # ----

            def create_table(self, engine):

                if not engine:
                    self.logger.error('create_table: please supply engine')
                    return False

                try:
                    DBObj.metadata.create_all(engine)

                except Exception as e:
                    print("ERROR: Can't create table for GutterRow: {0}".format(e))

        # ==== end StorageRow class ====

        # save in cache
        self.storage_models_cache[table_name] = StorageRow

        return StorageRow

    # ----

    def get_history_model(self, table_name):

        # get a model for a row in which gutter saves all data

        if self.storage_history_models_cache.get(table_name) is not None:
            return self.storage_history_models_cache.get(table_name)

        TABLE_POST_STRING = "_history"

        # ==== dynamic StorageHistoryRow class ====

        class StorageHistoryRow(DBObj):

            # basic model for saving all data rows

            __tablename__ = table_name + TABLE_POST_STRING  # to extend
            __table_args__ = {"schema": "gutter_data"}  # hack to specify schema

            id = Column(Integer(), primary_key=True)  # string
            row_id = Column(String())  # string
            valid_from = Column(DateTime)
            valid_to = Column(DateTime)
            pipeline_id = Column(Integer())
            data = Column(JSONB())

            # ----

            def __init__(self, row_id=None, valid_from=None, valid_to=None, pipeline_id=None, data=None):

                # NOTE: can be without parameters to only create table

                self.row_id = str(row_id)
                self.valid_from = valid_from
                self.valid_to = valid_to
                self.pipeline_id = pipeline_id
                self.data = data

            # ----

            def __repr__(self):
                # string representation of object
                return "<GutterHistoryRow id='{0}', valid_from='{1}', valid_from='{2}', " \
                       "pipeline_id='{3}', data='{4}', datahash='{5}'>".format(
                        self.id, self.valid_from, self.valid_to, self.pipeline_id, self.data)

            # ----

            def to_dict(self):

                # todo: make this more robust?
                ALLOWED_PROPERTIES_OUTPUT = [str, bool, str, int, float, long, dict, list]

                # simple filter out internal sql alchemy keys
                d = {}

                for key in self.__dict__.keys():

                    v = self.__dict__[key]

                    if type(v) in ALLOWED_PROPERTIES_OUTPUT:  # key[0] != '_': # first char not '_'
                        if (type(v) is str) and v is not None:
                            d[key] = v.encode('utf8', errors='ignore')  # force utf_8 encoding
                        else:
                            d[key] = v

                return d

            # ----

            def to_json(self):
                return json.dumps(self.to_dict())

            # ----

            def create_table(self, engine):

                if not engine:
                    self.logger.error('create_table: Please supply engine')
                    return False

                try:
                    DBObj.metadata.create_all(engine)

                except Exception as e:
                    print("Error: can't create table for gutter_history_row: {0}".format(e))

        # ==== end dynamic StorageHistoryRow class ====

        self.storage_history_models_cache[table_name] = StorageHistoryRow

        return StorageHistoryRow

    # ----

    def add_rows(self, rows):

        self.db_session.add_all(rows)

    # ----

    def commit(self):

        self.db_session.commit()

    # ----

    def get_data_by_id(self, table_name=None, id=None):

        if table_name is None or id is None:
            self.logger.error("Cannot get data without table_name and id!")
            return None

        StorageModel = self.get_storage_model(table_name)  # NOTE: this returns a SQLAlchemy ORM class

        try:
            row_object = self.db_session.query(StorageModel).filter(StorageModel.id == id).first()

            return row_object

        except Exception as e:
            self.logger.error(e)

    # ----

    def get_data_list(self, table_name, schema_definition, select=None, filters=[], limit=None, offset=None,
                      order_by=None, order_by_type=None, format=None):

        # get list of data rows

        OPERATOR_MAP = {
            'eq': operator.eq,
            'le': operator.le,  # less than or equal
            'lt': operator.lt,  # less than
            'ne': operator.ne,  # not equal
            'ge': operator.ge,  # greater than or equal
            'gt': operator.gt,  # greater than
        }
        # see: https://docs.python.org/2/library/operator.html

        StorageModel = self.get_storage_model(table_name)  # NOTE: this returns a SQLAlchemy ORM class

        # filters: list of <column>:'name', logic: '>=', value : 'mora_1604983'
        real_filters = []

        for filter in filters:

            entry_logic = None
            column_name = filter.get('column')
            logic = filter.get('logic')
            filter_value = filter.get('value')

            if column_name != 'id' and not self.schema_definition_has_column(schema_definition, column_name):
                self.logger.error("Skipped column name '{0}' in $filters".format(filter['column']))
            else:

                if column_name is None or logic is None or filter_value is None:
                    self.logger.error("Skipped malformed filter")
                else:
                    # note: here we convert supplied filters to sql_alchemy queries
                    # indices: these are not trivial because we want to use the indices on the JSONB data field
                    if column_name == 'id':
                        entry_logic = OPERATOR_MAP[filter['logic']](getattr(StorageModel, column_name), filter_value)
                    else:
                        # note: we need to cast numbers to use index on these json values
                        column_type = self.get_column_type_from_schema_definition(schema_definition,
                                                                                  column_name)  # number or string
                        entry_logic = None
                        data_dict = getattr(StorageModel, 'data')

                        if column_type == 'number':
                            # astext to force _>> operator: https://stackoverflow.com/questions/29974143/python_sqlalchemy_and_postgres_how_to_query_a_json_element
                            entry_logic = OPERATOR_MAP[filter['logic']](
                                cast(self.get_model_data_path(data_dict, column_name).astext, Numeric), filter_value)
                        # timestamp
                        elif column_type == 'string' and schema_definition['properties'][column_name].get(
                                'format') == 'date_time':

                            # see: http://docs.sqlalchemy.org/en/latest/core/functions.html
                            class GUTTER_TO_TIMESTAMP(generic_function):
                                type = String

                            timestamp = None

                            try:
                                timestamp = datetime.datetime.strptime(filter['value'], '%Y_%m_%d% %H:%M:%S')
                            except:
                                try:
                                    timestamp = datetime.datetime.strptime(filter['value'], '%y_%m_%d')
                                except Exception as e:
                                    self.logger.error(e)

                            if timestamp is None:
                                self.logger.error("cannot parse timestamp filter format {0}".format(filter_value))
                            else:
                                entry_logic = OPERATOR_MAP[filter['logic']](
                                    func.gutter_to_timestamp(getattr(StorageModel, 'data')[column_name].astext),
                                    func.gutter_to_timestamp(filter_value))
                        # just a string
                        else:
                            entry_logic = OPERATOR_MAP[filter['logic']](
                                self.get_model_data_path(data_dict, column_name).astext, filter_value)

            if entry_logic is not None:
                real_filters.append(entry_logic)

        query = self.db_session.query(StorageModel).filter(*real_filters)
        

        # filter parameter: orderBy
        if order_by is not None:
            
            # for ordering to work we need to cast
            column_type = self.get_column_type_from_schema_definition(schema_definition, order_by) # number or string
            
            if order_by_type is "desc":
                
                if column_type == 'number':
                    query = query.order_by( desc( cast(StorageModel.data[order_by].astext, Numeric)), StorageModel.id) # NOTE: secondary order by id
                else:
                    # string
                    query = query.order_by( desc(StorageModel.data[order_by]), StorageModel.id)
                
            else:
                # ascending order
                if column_type == 'number':
                    query = query.order_by( cast(StorageModel.data[order_by].astext, Numeric), StorageModel.id) # NOTE: secondary order by id
                else:
                    # string
                    query = query.order_by( StorageModel.data[order_by], StorageModel.id)                

        else:
            # basic ordering by id
            # IMPORTANT: otherwise iterating over large sets give inconsistent results
            query = query.order_by(StorageModel.id)
                
        # filter parameter: offset 
        if offset is not None:
            if isinstance(offset, int):
                query = query.offset(offset)

        # limit
        if limit is not None:
            if isinstance(limit, int):
                # we can get maximum of GET_MAX_ROWS otherwise get GET_MAX_ROWS_DEFAULT 
                if limit > self.GET_MAX_ROWS:
                    limit = self.GET_MAX_ROWS
                query = query.limit(limit)
        else:
            query = query.limit(self.GET_NUM_ROWS_DEFAULT)

        # DEBUG: print sql
        # print(query.statement.compile(compile_kwargs={"literal_binds": True}))
        # NOTE: this is not always the right sql _ it seams that there dialects are handled after this step

        list = query.all()  # returns objects
        list_dicts = [r.get_data_dict() for r in list]

        # call parameter: format: enable geojson output for gis applications
        if format == 'geojson':
            geojson = self.data_to_geo_json(data=list_dicts, schema_definition=schema_definition)
            return geojson
        else:
            # just normal json
            return list_dicts

    # ----

    def insert_data(self, table_name=None, user=None, data=None):

        if table_name is None or data is None or user is None:
            self.logger.error("Data put failed without table_name and/or data!")
            return None

        # concerning id's: either it is in the supplied data and we check and save it, or it is automatically generated
        
        # NOTE: input checking with schema definition is done by RequestHandler
        StorageModel = self.get_storage_model(table_name)  # NOTE: this returns a SQLAlchemy ORM class
        
        id = data.get('id')
        
        if id is not None:
            # user supplied an id: we need to check if this does not exist!
            self.logger.info("User supplied an own id: check if it exists before saving!")
            existing_storage_row = self.db_session.query(StorageModel).filter(StorageModel.id == id).first()
            if existing_storage_row:
                self.logger.info("Row with id '{0}' already exists!".format(id))
                return GutterStoreError(msg="Row with id '{0}' already exists!".format(id), status_code=500)
        
        # NOTE: if id and created_at, last_checked, last_updated are default values are generated in creation of StorageRow instance
        new_storage_row = StorageModel(id=id, created_by=user, created_at=data.get('created_at'), last_checked=None, last_updated=None, pipeline_id=None, data=data)
        self.add_rows([new_storage_row])
        self.commit()

        # return new row instance
        return new_storage_row

    # ----

    def update_data(self, table_name=None, data=None):

        if table_name is None and data is None:
            self.logger.error("Data put failed without table_name, id and/or data!")
            return None

        # json data needs to contain an id
        id = data.get('id')  # id should be in json input
        if id is None:
            self.logger.error("Cannot update a row without an id!")
            return None

        # NOTE: if id is None one will be generated
        existing_storage_row = self.get_data_by_id(table_name=table_name, id=id)

        if existing_storage_row is None:
            self.logger.error("Cannot get a row in table {0} with id {1}".format(table_name, id))
            return None

        existing_storage_row.data = data  # save new data
        self.db_session.commit()

        # return updated row
        return existing_storage_row

    # ----

    def delete_data(self, table_name, id):

        if table_name is None and id is None:
            self.logger.error("Data delete failed without table_name and id")
            return None

        existing_storage_row = self.get_data_by_id(table_name=table_name, id=id)

        if existing_storage_row is None:
            self.logger.error("No row found with id {0}".format(id))
            return False

        # do the delete
        try:
            self.db_session.delete(existing_storage_row)
            self.db_session.commit()

            return True

        except Exception as e:
            self.logger.error("Error deleting row with id '{0}': {1}".format(id, e))

        return False

    # ==== special formats of data: for now only geo ====

    def data_to_geo_json(self, data=[], schema_definition=None):

        # transforms list of dict data rows into geojson format 

        if schema_definition is None:
            self.logger.error("No schema definition given: original data returned")
            return data

        wkt_property_name, srid = self.find_wkt_property_and_srid(data, schema_definition)

        if wkt_property_name is None:
            return data
        else:

            geojson_features = []

            for row in data:
                # iterate over rows
                geom = shapely.wkt.loads(row.get(wkt_property_name))

                if srid != 4326:
                    project = partial(pyproj.transform,
                                      pyproj.Proj(init='epsg:' + str(srid)),  # source coordinate system
                                      pyproj.Proj(init='epsg:4326'))  # destination coordinate system

                    geom = transform(project, geom)  # apply projection

                geojson_feature = geojson.Feature(geometry=geom,
                                                  properties=row)  # just add whole row to geojson properties for ease and verification

                geojson_features.append(geojson_feature)

            g = {"type": "FeatureCollection", "features": geojson_features}

            # return data as geojson features with properties
            return g

    # ----

    def find_wkt_property_and_srid(self, data=[], schema_definition=None):

        # return property name of wkt and srid

        if type(schema_definition) is not dict:
            self.logger.error("No schema definition given: cannot get wkt colunn")
            return None

        SAMPLE_ROWS = 10
        WKT_KEYWORDS = ["POLYGON", "LINESTRING", "POINT", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON"]

        # find string properties from schema_definition ( others can't contain wkt strings )
        string_property_names = self.find_string_properties_in_schema_definition(schema_definition)

        # go sample and detect wkt strings
        wkt_property_names = []

        for row in data[0:SAMPLE_ROWS]:
            for prop_name in string_property_names:
                v = row.get(prop_name)
                if type(v) is str:
                    if any(wkt_keyword in v for wkt_keyword in WKT_KEYWORDS):
                        if prop_name not in wkt_property_names:
                            wkt_property_names.append(prop_name)

        if len(wkt_property_names) == 0:
            return None

        lng_lat_column = self.try_to_find_lng_lat_wkt_column(wkt_property_names)

        if lng_lat_column:
            return lng_lat_column, 4326  # return property name of wkt and srid
        else:
            # just pick the first and get srid
            first_wkt_property = wkt_property_names[0]

            for row in data[0:SAMPLE_ROWS]:
                wkt_string = row.get(first_wkt_property)
                if type(wkt_string) is str:
                    srid = self.get_srid_of_wkt_value(wkt_string)
                    break

            return first_wkt_property, srid

    # ----

    def find_string_properties_in_schema_definition(self, schema_definition=None):

        # NOTE: only at first level: not in nested objects

        if schema_definition is None:
            return None

        string_property_names = []

        for prop_name, prop_obj in schema_definition['properties'].iteritems():
            if prop_obj.get('type') == 'string':
                string_property_names.append(prop_name)

        return string_property_names

    # ----

    def try_to_find_lng_lat_wkt_column(self, wkt_property_names=[], data=[]):

        if len(wkt_property_names) == 0 or len(data) == 0:
            return None
        else:
            # we got wkt_property_names, see if there is one that has clear lng,lat coordinates
            for wkt_property in wkt_property_names:
                for row in data:
                    wkt_string = row.get(wkt_property)
                    if self.is_lng_lat_wkt(wkt_string) is True:
                        return wkt_property

    # ----

    def is_lng_lat_wkt(self, wkt_string):

        matches = re.findall('([\d]+\.[\d]+) ([\d]+\.[\d]+)', wkt_string)

        x_coords = [m[0] for m in matches]
        y_coords = [m[1] for m in matches]

        is_lng_lat = True

        # IMPORTANT: lng,lat in NL for now !!
        for c in range(len(x_coords)):
            if not (x_coords[c] > 2.0 and x_coords[c] < 10.0):
                is_lng_lat = False
                break
            if not (y_coords[c] > 40.0 and y_coords[c] < 60.0):
                is_lng_lat = False
                break

        return is_lng_lat

    # ----

    def get_srid_of_wkt_value(self, wkt_string):

        # NOTE: only do 28992 and 900913
        # SRID 28992 _ range x : 646.36 - 308975.28  range y:  276050.82 - 636456.31
        # SRID 900913 _ range x: -20026376.39 -20048966.10 range y: 20026376.39 20048966.10

        # make it simple: only distinction: y_coordinate for 900913 in NL is above 1000000

        coords = re.findall('([\d]+\.[\d]+) ([\d]+\.[\d]+)', wkt_string)

        y_coords = [m[1] for m in coords]

        is_sm = False

        # NOTE: lng,lat in NL for now
        for c in range(len(y_coords)):
            try:
                y = int(y_coords[c])
                if y > 1000000:
                    is_sm = True
                    break
            except Exception as e:
                pass

        if is_sm:
            return 900913
        else:
            return 28992

    # ==== UTILS schema definitions ====

    def schema_definition_has_column(self, schema_definition=None, column_name=None):

        # IMPORTANT: column_name can be nested like "field.othercolumn": detect and verify all the same

        if schema_definition is None and column_name is None:
            self.logger.error("schema_definition_has_column need input 'schema_defininition' and 'column_name' !")
            return False

        columns = column_name.split('.')

        if len(columns) == 0:
            return column_name in schema_definition['properties'].keys()
        else:
            # nested column name: iterate and test
            try:
                cur_level = schema_definition['properties']
                for col_name in columns:
                    if col_name not in cur_level.keys():
                        return False
                    if col_name != columns[-1]:  # not the last
                        cur_level = cur_level[col_name]['properties']  # next level of schema
            except Exception as e:
                self.logger.error(e)
                return False

        return True

    # ----

    def get_column_type_from_schema_definition(self, schema_definition, column_name):

        # NOTE: columnname can be nested 'field.field2' 

        if not self.schema_definition_has_column(schema_definition, column_name):
            return None

        try:
            columns = column_name.split('.')
            cur_level = schema_definition['properties']
            for col_name in columns:
                if col_name not in cur_level.keys():
                    self.logger.error("get_column_type_from_schema_definition: no key like: {0}".format(col_name))
                    return None
                if col_name != columns[-1]:  # not the last
                    cur_level = cur_level[col_name]['properties']  # next level of schema
                else:
                    return cur_level[col_name]['type']
        except Exception as e:
            self.logger.error(e)
            return None

        if schema_entry is not None:
            return schema_entry['type']
        else:
            self.logger.error("get_column_type_from_schema_definition: unknown column_name: {0}".format(column_name))
            return None

            # ==== UTILS models and data ====

    def get_model_rows_by_ids(self, model, ids=[]):

        """ Get the rows of a given ORM Class Model with specific ids
        
        :param model: SQLAlchemy ORM Model class
        :param primary_key_name: which column is the primary key
        :param ids: List of ids
        :return: list of ORM Model rows
        
        """

        ids = [str(id) for id in ids]
        rows = self.db_session.query(model).filter(getattr(model, 'id').in_(ids)).all()

        if len(rows) != len(ids):
            row_ids = [getattr(row, 'id') for row in rows]
            missing_ids = [id for id in ids if id not in row_ids]
            self.logger.warn("""get_model_rows_by_ids: Found {0}/{1} rows. 
            Search ids [{2}]
            Missing ids: [{3}]
            Present ids: [{4}]""".format(len(rows), len(ids), ",".join(ids), ",".join(missing_ids), ",".join(row_ids)))

        return rows

    # ----

    def table_is_present(self, table_name):

        # utility function: using database object from gutter_flow to see if a table already exists
        """ DISABLED
        db = Database(**self.connection_data)
        table_names = db.get_table_and_view_names(schema='gutter')
        
        return table_name in table_names
        
        """

    # ----

    def create_indices(self, table_name=None, schema_definition=None):

        if table_name is None or schema_definition is None:
            self.logger.error("create_indices failed: missing parameters table_name or schema_definition")
            return False

        # PROBLEMS WITH BIG TEXT: We encountered erros making indices for large text values
        # SOLUTION: let those sql fail seperately by executing every sql seperately

        sqls = []  # make_sqls_for_indices recurses and add sqls to the list reference
        self.make_sqls_for_indices(table_name=table_name, properties=schema_definition['properties'], sqls=sqls)

        for sql in sqls:
            try:
                r = self.db_session.execute(sql)
                self.db_session.commit()
            except Exception as e:
                self.db_session.rollback()  # roll back error query
                self.logger.error(e)

    # ----

    def make_sqls_for_indices(self, table_name=None, properties=None, sqls=[], parent_names=[]):

        # IMPORTANT: Recursion is used to index nested fields!  
        # for every property in schema definition we create an index in the data jsonb field
        # properties is key,val

        if table_name is None or properties is None:
            self.logger.error("make_sqls_for_indices: Please supply table_name and properties!")
            return []

        sqls = []

        for property_name, property_def in properties.items():

            index_name = "gutter_" + table_name + '_' + '_'.join(parent_names) + "_" + property_name + "_idx"

            # Index on a JSONB field : 
            # create index if not exists locatie_idx on waarnemingen_real using btree ( cast( (data#>>'{locatie,latitude}') as numeric) )
            # NOTE: we still use the _>> access for single _ non nested fields

            if len(parent_names) == 0:
                primary_field = True
            else:
                primary_field = False

            json_path = '{' + ','.join(parent_names + [property_name]) + '}'

            # we have different properties that need different indices: strings, numbers or datetime

            # !!!! IMPORTANT: GUTTER_TO_TIMESTAMP !!!!
            # check and make that function on database

            if property_def.get('type') == 'string' and property_def.get('format') == 'date_time':
                # NOTE: on time zones: http://blog.untrod.com/2016/08/actually_understanding_timezones_in_postgresql.html 
                # UTC+2
                if primary_field:
                    sqls.append(
                        "CREATE INDEX IF NOT EXISTS {0} ON {1} USING BTREE ( GUTTER_TO_TIMESTAMP(data->>'{2}') )".format(
                            index_name, table_name, property_name));
                else:
                    sqls.append(
                        "CREATE INDEX IF NOT EXISTS {0} ON {1} USING BTREE ( GUTTER_TO_TIMESTAMP(data#>>'{2}') )".format(
                            index_name, table_name, json_path));

            elif property_def.get('type') == 'number':

                if primary_field:
                    sqls.append(
                        "CREATE INDEX IF NOT EXISTS {0} ON {1} USING BTREE ( cast(data->>'{2}' as numeric ) )".format(
                            index_name, table_name, property_name));
                else:
                    sqls.append(
                        "CREATE INDEX IF NOT EXISTS {0} ON {1} USING BTREE ( cast(data#>>'{2}' as numeric) )".format(
                            index_name, table_name, json_path));

            elif property_def.get('type') == 'object':
                # RECURSE
                #  properties to make indices for properties of objects
                new_parent_names = parent_names + [property_name]
                self.make_sqls_for_indices(table_name, property_def['properties'], sqls, new_parent_names)

            else:  # string
                if primary_field:
                    sqls.append(
                        "CREATE INDEX IF NOT EXISTS {0} on {1} USING BTREE ( (data_>>'{2}') )".format(index_name,
                                                                                                      table_name,
                                                                                                      property_name));
                else:
                    sqls.append(
                        "CREATE INDEX IF NOT EXISTS {0} on {1} USING BTREE ( (data#>>'{2}') )".format(index_name,
                                                                                                      table_name,
                                                                                                      json_path));

        # check if queries use indices:
        # select gutter.waarnemingen_real.id, gutter.waarnemingen_real.created_at, gutter.waarnemingen_real.last_checked, gutter.waarnemingen_real.last_updated, gutter.waarnemingen_real.pipeline_id, gutter.waarnemingen_real.data, gutter.waarnemingen_real.datahash  from gutter.waarnemingen_real  where (data #>> '{locatie,latitude}')::numeric < 5
        # select gutter.waarnemingen_real.id, gutter.waarnemingen_real.created_at, gutter.waarnemingen_real.last_checked, gutter.waarnemingen_real.last_updated, gutter.waarnemingen_real.pipeline_id, gutter.waarnemingen_real.data, gutter.waarnemingen_real.datahash 
        # from gutter.waarnemingen_real where cast(((gutter.waarnemingen_real.data#>>'{locatie,longitude}')) as numeric) < '4.9' and cast(((gutter.waarnemingen_real.data#>>'{locatie,latitude}')) as numeric) > '52'
        # note: with limit no index is used !

        return sqls

        # ----

    def drop_indices(self, table_name, schema_definition):

        if table_name is None or schema_definition is None:
            self.logger.error("Create_indices failed: missing parameters table_name or schema_definition")
            return False

        sqls = []

        for property_name, property_def in schema_definition['properties'].iteritems():
            index_name = "gutter_" + table_name + "_" + property_name + "_idx"

            sqls.append("DROP INDEX IF EXISTS {0}".format(index_name, table_name, property_name));

        sql = ";\n".join(sqls)

        try:
            r = self.db_session.execute(sql)
            self.db_session.commit()
        except Exception as e:
            self.logger.error(e)

            # ----

    def create_data_view(self, table_name=None, schema_definition=None):

        """
            The Data is JSON format is hard to look into: Generate a view in Postgres to fix that!
        
        """

        if table_name is None:
            self.logger.error("cannot create view. no table given!")
            return False

        if schema_definition is None:
            self.logger.error("Cannot create view for table '{0}' with schema definition!".format(table_name))
            return False

        # NO RECURSION YET:
        # for now only do first level ( no recursion through possible nested fields )
        cols = [("data_>>'" + property_name + "' AS " + property_name) for property_name in
                schema_definition['properties']]

        sql = "CREATE VIEW {0} AS SELECT {1} FROM {2}".format(table_name + "_view", ",".join(cols), table_name)

        self.logger.info("create_data_view: {0}".format(sql))

        try:
            r = self.db_session.execute(sql)
            self.db_session.commit()
        except Exception as e:
            self.logger.error(e)

    # ----

    def get_model_data_path(self, d, path_str):

        """"
            We use this to enable nested filters (location.longitude)
            this function iterates the main object d to the specific path of the nested data
            NOTE: we can't use 'get' because the objects are not dicts
        
        """

        properties = path_str.split('.')

        cur_level = d

        for p in properties:

            if p == properties[-1]:  # last one
                return cur_level[p]
            else:
                cur_level = cur_level[p]

                if cur_level is None:  # no key like that
                    self.logger.error("get_dict_data_path: unknown key: {0}".format(p))
                    return None

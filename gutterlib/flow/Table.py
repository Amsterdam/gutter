'''
created on 29 mei 2018

@author: mark
'''

import re
import datetime

from sqlalchemy.schema import MetaData
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import inspect
from sqlalchemy.schema import Table as SQLAlchemyTable

from sqlalchemy import Column as SQLAColumn, Integer as SQLAInteger, String as SQLAString, Numeric as SQLANumeric, \
    DateTime as SQLADateTime
from sqlalchemy.dialects.postgresql import JSONB as SQLA_JSONB, ARRAY as SQLA_ARRAY, NUMERIC as SQLA_NUMERIC
from sqlalchemy.orm import sessionmaker as SQLAlchemySessionMaker

from sqlalchemy.ext.declarative import declarative_base

from decimal import Decimal

import logging


class Table:

    def __init__(self, database, schema, name):

        self.database = database  # Gutter Database class instance
        self.name = name
        self.columns = {}  # name : { type ( type in db ), description, primary }
        self.schema = schema  # note: distinction between schema of database and json schema
        self.metadata = None

        self.schema_definition = None  # json schema generated from table structure and content
        self.primary_key = None
        self.model_class = None  # a SQLAlchemy ORM model class definition
        self.session = None

        self.create_logger()

        self.setup()

    # ----

    def __repr__(self):

        return '<Table database="{0}", name="{1}", schema="{2}">'.format(
                self.database.name,
                self.name,
                self.schema)

    # ----

    def setup(self):

        self.setup_session()

    # ----

    def setup_session(self):

        try:
            session_maker = SQLAlchemySessionMaker()
            session_maker.configure(bind=self.database.engine)  # Gutter Database class has a SQLAlchemy database engine
            self.session = session_maker()
            self.session.autoflush = True

            return True

        except Exception as e:
            self.logger.error('error setup session: {0}'.format(e))
            return False

    # ----

    def get_columns(self, with_descriptions=False):

        """ Get list of column definitions for this table and populates self.columns
        
        :returns list of  dict { type, description ( database definitions like VARCHAR, TEXT etC) , primary }
        
        """

        if not self.database:
            self.logger.error("Cannot get column info: no database given")
            return []

        try:
            # NOTE: We need to specifically reflect the schema to be allowed to find the table
            if self.schema is None:
                return []  # return empty list

            # find out columns
            self.metadata = MetaData(bind=self.database.engine, reflect=True, schema=self.schema)
            sqla_table = SQLAlchemyTable(self.name, self.metadata, autoload=True, autoload_with=self.database.engine)
            insp = Inspector.from_engine(self.database.engine)
            insp.reflecttable(sqla_table, None)

            descriptions = {}

            if with_descriptions:
                # extra description
                descriptions = self.get_column_descriptions()  # { colname : desc }

            for c in sqla_table.columns:
                self.columns[c.name] = {'type': self.SQLA_column_type_to_string(c.type),
                                        'description': descriptions.get(c.name),
                                        'primary': self.probably_is_primary_key(c)}

            return self.columns

        except Exception as e:
            self.logger.error("Cannot get columns of table '{0}.{1}': {2}".format(self.schema, self.name, e))
            return []

    # ----

    def SQLA_column_type_to_string(self, SQLA_column_type):

        """ SQL alchemy defines type in a class definition: make sure this is a string
        
        :returns: string or None -- 
        
        """

        column_type = None
        try:
            column_type = str(SQLA_column_type)  # for NullType() ( unknown in SQLA)  this gives a error
        except Exception as e:
            # catch unknown type
            pass

        return column_type

    # ----

    def get_column_names(self):

        if len(self.columns) == 0:
            self.get_columns()

        return list(self.columns.keys())

    # ----

    def probably_is_primary_key(self, SQLAColumn):

        """ for database views ( which behave a lot like table ) we cannot find the primary key, 
            do this based on some simple checks
            
        :param SQLAColumn: SQLAlchemy Column instance with:  type, name, primary_key
        :returns: Boolean -- True of False
         
        """

        # this is sure
        if SQLAColumn.primary_key:
            return True

        # otherwise make a guess based on name
        PROBABLY_ID_COLUMN_NAMES = ["^id", "$[a_za_z_]+id^"]

        for r in PROBABLY_ID_COLUMN_NAMES:
            results = re.findall(r, SQLAColumn.name.lower())

            if len(results):
                self.logger.info("Find column '{0}' to be a primary key".format(SQLAColumn.name))
                return True

        return False

    # ----

    def get_columns_alt(self):
        # NOT TESTED: this uses inspector but has less information
        insp = inspect(self.database.engine)
        return insp.get_columns(self.name, self.schema)

    # ----

    def get_column_descriptions(self):

        """ SQLAlchemy doesn't get the meta description of columns
            So here we have some custom SQL queries to retrieve those
              
        :returns: dict -- { colname : description, colname2 : description }
        
        """

        try:
            # manually query for column descriptions
            if 'postgres' in self.database.type:
                columns = self.get_columns_pg()  # return dictionary with colname : { type, description }
                col_descriptions = {}
                for key, val in columns.items():
                    col_descriptions[key] = val.get('description')
                return col_descriptions
            else:
                if 'oracle' in self.database.type:
                    # TODO
                    return
        except Exception as e:
            self.logger.error(e)
            return {}

    # ----

    def get_columns_pg(self):

        """
            Get column info direct with SQL for Postgres

            :returns: dict -- name : { table, name, type, description }
        
        """

        sql =   """
                    SELECT table_schema as schema,
                           table_name as table,
                           column_name as name,
                           upper(data_type) as type,
                               (SELECT description from pg_catalog.pg_description 
                               WHERE objsubid = ordinal_position 
                               AND objoid = 
                                    (SELECT oid FROM pg_class WHERE relname = '{0}' AND relnamespace = 
                                        (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = '{1}')
                                    )
                               ) 
                            FROM information_schema.columns WHERE table_name='{0}'
                """.format(self.name, self.schema)

        r = self.database.engine.execute(sql)

        column_names = r.keys()
        rows = r.fetchall()

        col_objs = [dict(zip(column_names, row)) for row in rows]

        column_descriptions = {}
        for col_obj in col_objs:
            column_descriptions[col_obj['name']] = {'type': col_obj.get('type'),
                                                    'description': col_obj.get('description'),
                                                    'table': col_obj.get('table'), 'schema': col_obj.get('schema')}

        return column_descriptions

    # ----

    def get_columns_ORC(self):

        # NOTE: NOT TESTED
        # see: https://stackoverflow.com/questions/10912337/how_to_show_comments_of_a_column_with_desc_operation

        sql = """SELECT tc.column_name,
                        tc.nullable,
                        tc.data_type || case when tc.data_type = 'number' and tc.data_precision is not null then '(' || tc.data_precision || ',' || tc.data_scale || ')'
                            when tc.data_type like '%char%' then '(' || tc.data_length || ')'
                            else null
                        end type,
                        cc.comments
                        FROM  user_col_comments cc
                        JOIN   user_tab_columns  tc on  cc.column_name = tc.column_name
                            and cc.table_name  = tc.table_name
                        WHERE  cc.table_name = upper('{0}')""".format(self.name)

        r = self.database.engine.execute(sql)

        return r.fetchall()

    # ----

    def get_schema_definition(self):

        """ Get JSON Schema definition from columns/metadata for this table in the database
        
        :returns: dict -- JSON schema
        
        """

        if len(self.columns) == 0:
            self.get_columns()  # dicts:  { 'type', 'description', 'primary' }

        self.schema_definition = self.map_columns_to_schema_definition(self.columns)

        return self.schema_definition

    # ----

    def map_columns_to_schema_definition(self, columns=None):

        """ Get JSON Schema definition from column definitions
        
        :param columns: List of column data { name, type, primary }
        :returns: dict or None -- JSON schema
        
        """

        if columns is None:
            columns = self.columns

        if len(columns) == 0:  # NOTE: len(dict) works also
            self.logger.error("Could not get schema from table '{0}': "
                              "does it exist in schema '{1}'?".format(self.name, self.schema))
            return None

        # MAP from database type to JSON Schema type
        COLUMN_DBTYPE_TO_JSONSCHEMA = {
            'VARCHAR': {'type': 'string'},
            'TEXT': {'type': 'string'},
            'INTEGER': {'type': 'integer'},
            'NUMBER': {'type': 'number'},  # oracle
            'FLOAT': {'type': 'number'},  # pg
            'DOUBLE': {'type': 'number'},  # pg
            'NUMERIC': {'type': 'number'},  # pg
            'DECIMAL': {'type': 'number'},  # pg
            'JSONB': {'type': 'object'},  # pg
            'JSON': {'type': 'object'},  # pg
            'ARRAY': {'type': 'array'},  # pg
            'BIGINT': {'type': 'integer'},
            'TIMESTAMP': {'type': 'string', 'format': 'date-time'},  # pg
            'DATE': {'type': 'string', 'format': 'date-time'},  # oracle
            'CLOB': {'type': 'string'},  # oracle
            'TINYINT': {'type': 'integer'},  # mysql
        }

        json_properties = {}
        primary_keys = [];

        # cycle through columns 
        for col_name, col_obj in columns.items():

            json_prop = {}

            # map database column type to JSON schema type
            found_type = False
            cur_col_type = col_obj.get('type')

            if cur_col_type is None:
                self.logger.warning("Column '{0}' has unknown type: skipped for JSON schema!".format(col_name))
            else:
                for db_type, json_schema_def in COLUMN_DBTYPE_TO_JSONSCHEMA.items():

                    try:

                        if db_type.lower() in cur_col_type.lower():
                            json_properties[col_name] = json_schema_def
                            found_type = True
                            break

                    except Exception as e:
                        self.logger.error(e)
                        # break

                if not found_type:
                    self.logger.warning("Could not get JSON SCHEMA type for column name: {0}, type: {1}".format(
                                        col_name,
                                        col_obj.get('type')))

                    # keep track of primary keys
            if col_obj.get('primary'):
                primary_keys.append(col_name)

        # JSON Schema general structure 
        schema = {
            'title': self.name,
            'properties': json_properties,
            'required': primary_keys
        }

        # extra field to define primary keys
        if len(primary_keys):
            schema['primaryKey'] = ','.join(primary_keys)

        return schema

    # ----

    def get_model(self, manual_primary_key=None):

        """ get dynamic SQLAlchemy ORM class based on data schema definition
            There can be two sources of the model: 
                - either a given user_schema or
                - one automatically generated from table in database: table_schema
                
            :param manual_primary_key: Overide primary key with the name of a column
            :type manual_primary_key: str
            :returns: SQLALChemy Model Class or None -- 
        
        """

        self.logger.info("Get model for table {0}".format(self.name))

        if self.model_class is not None:
            # cached
            return self.model_class

        if not self.schema_definition:
            self.get_schema_definition()

        if self.schema_definition is None:
            self.logger.error("Could not get model for table: '{0}'".format(self.name))
            return None

        Base = declarative_base()

        self.model_class = type(str(self.name), (Base,), self.generate_SQLA_definition_dict(
            primary_key=manual_primary_key))  # str to avoid type_error: type() argument 1 must be string, not unicode

        return self.model_class

    # ----

    def set_primary_key(self, key_name):

        self.primary_key = key_name

    # ----

    def generate_SQLA_definition_dict(self, primary_key=None):

        """ Generate a dictionary with sql_alchemy definition of columns and the __repr__ function
            
            :param primary_key: Overide primary key with name of specific column
            :returns: dict or None -- 
        
        """

        if self.schema_definition is None:
            self.logger.error("Cannot generate a SQLA definition without schema_definition. Generate that first!")
            return None

        ALLOWED_PROP_TYPES = [str, bool, str, int, float, dict, list, datetime.datetime, Decimal]

        if primary_key is not None:
            self.logger.info("Generate SQL Definition dict: Manual primary key given: '{0}'".format(primary_key))

        model_name = self.schema_definition['title']  # set name of model

        # ==== create repr function for new ORM model class
        def class_repr(self):
            # slightly hacked outputting of columns based on crude selection
            if self:
                prop_names = self.get_props()
                prop_vals = ([unicode(getattr(self, p)) for p in prop_names])
                prop_names_vals = ','.join([kv[0] + "='" + kv[1] + "'" for kv in zip(prop_names, prop_vals)])

            else:
                prop_names_vals = " <no vals>"

            return "<{0} ({1})>".format(model_name, prop_names_vals)

        # ==== model_class get_props
        def get_props(self):

            prop_names = [p for p in dir(self) if type(getattr(self, p)) in ALLOWED_PROP_TYPES and p[0] != '_']
            excluded_candidates = [p + "=" + unicode(type(getattr(self, p))) for p in dir(self) if
                                   type(getattr(self, p)) not in allowed_prop_types and type(getattr(self, p)) not in [
                                       method_type, meta_data] and p[0] != '_']
            if len(excluded_candidates) > 0:
                print ("data_obj {0}: check ALLOWED_PROP_TYPES if one of these columns need to be outputed {1}".format(
                    model_name, ",".join(excluded_candidates)))

            return prop_names

        # ==== model_class get_dict
        def to_dict(self):

            # simple filter out internal sql alchemy keys
            d = {}

            for key in self.get_props():
                v = getattr(self, key)
                d[key] = unicode(v).encode('utf8', errors='ignore')  # force utf_8 encoding

            return d

            # ==== end of function definition for object

        # structure of SQLAlchemy ORM Model class defined in dict
        class_props = {'__tablename__': self.name,
                       '__schema__': self.schema,  # for pg
                       '__table_args__': {'schema': self.schema},  # for oracle
                       '__repr__': class_repr,
                       'to_dict': to_dict,
                       'get_props': get_props}  # base

        # gather properties from schema definition

        for prop_name, prop_def in self.schema_definition['properties'].items():

            prop_type = prop_def.get('type') or 'string'
            prop_format = prop_def.get('format')

            if prop_type == 'string' and prop_format == 'date_time':
                class_props[prop_name] = SQLAColumn(SQLADateTime)
            elif prop_type in ['integer']:
                class_props[prop_name] = SQLAColumn(SQLAInteger)
            elif prop_type in ['number']:
                class_props[prop_name] = SQLAColumn(SQLANumeric)
            elif prop_type == 'string':
                class_props[prop_name] = SQLAColumn(SQLAString)
            elif prop_type == 'object':
                class_props[prop_name] = SQLAColumn(SQLA_JSONB)
            elif prop_type == 'array':
                class_props[prop_name] = SQLAColumn(SQLA_ARRAY)
            else:
                self.logger.error('generate_SQLA_definition_dict: Error unknown column type: {0}'.format(prop_type))

            # set primary key in model
            if primary_key is not None:
                if prop_name == primary_key:
                    class_props[prop_name].primary_key = True
                    primary_key_is_set = True
            else:
                if prop_name in self.schema_definition['required']:
                    class_props[prop_name].primary_key = True
                    primary_key_is_set = True

        # some checks:
        if not primary_key_is_set:
            self.logger.error(
                'Table could not set primary key! without no orm model can be made. '
                'either make it trivial ( like id ) or set the primary key in pipeline object')

        return class_props

    # ----

    def start_query(self):

        """ Using a Model Class we can start a direct query 
        
        """

        if not self.model_class:
            self.get_model(manual_primary_key=self.primary_key)

        if self.model_class is None:
            self.logger.error("could not start query for table '{0}'".format(self.name))
            return False

        query = self.session.query(self.model_class).order_by(
            self.model_class.id)  # NOTE: we force ordering on id for stability

        return query  # needs to be finished with all() or first() and limit() etc

    # ==== utils ==== #

    def create_logger(self):

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

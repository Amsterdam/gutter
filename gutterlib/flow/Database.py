"""
    created on 29 mei 2018

    @author: mark
"""

import logging
import os

from sqlalchemy import create_engine
from sqlalchemy import inspect

from .Table import Table


class Database:

    def __init__(self, db_type, url, port, name, user, password):

        self.db_type = db_type
        self.url = url
        self.name = name
        self.port = port
        self.user = user
        self.password = password

        self.connection_string = None
        self.engine = None
        self.failed_connection = True
        self.inspector = None  # inspector class: http://docs.sqlalchemy.org/en/latest/core/reflection.html
        self.table_and_view_names = []  # includes schemas ( schema.tablename )
        self.view_names = []  # database views
        self.schema_names = []

        self.create_logger()

        # init
        self.connect()

    # ----

    def __repr__(self):

        return "<Database type='{0}', url='{1}', " \
               "port='{2}', name='{3}', user='{4}'>".format(
                self.db_type,
                self.url,
                self.port,
                self.name,
                self.user)

    # ----

    def __del__(self):
        # cleanup all connections
        if self.engine:
            try:
                self.engine.dispose()
            except Exception as e:
                pass

    # ----

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

    def connect(self):

        try:
            # oracle needs an extra SID connection string
            if 'oracle' in self.db_type:
                import cx_oracle

                # !!!! IMPORTANT: this does not work because it is active in an own shell !!!!
                # set search path to c++ library in ./oraclelib
                oracle_lib_paths = [os.path.realpath('../oraclelib'), os.path.realpath('./oraclelib')]
                if os.environ.get("LD_LIBRARY_PATH"):
                    oracle_lib_paths.append(os.environ.get("LD_LIBRARY_PATH"))
                os.environ["LD_LIBRARY_PATH"] = ":".join(oracle_lib_paths)
                self.logger.info("Database set search path to oracle_lib: {0}".format(os.environ["LD_LIBRARY_PATH"]))

                dns_str = cx_Oracle.makedsn(self.url, self.port, self.name)
                dns_str = dns_str.replace('SID', 'SERVICE_NAME')  # oracle uses sid's instead of database names
                self.connection_string = 'oracle://{0}:{1}@{2}'.format(self.user, self.password, dns_str)
            else:
                # tested with postgresql and mysql
                self.connection_string = '{0}://{1}:{2}@{3}:{4}/{5}'.format(self.db_type, self.user, self.password,
                                                                            self.url, self.port, self.name)

            # create engine and connect
            self.engine = create_engine(self.connection_string, echo=False)

            self.inspector = inspect(self.engine)
            self.failed_connection = False
            self.logger.info("Connected to foreign database!")

        except Exception as e:
            self.logger.error("Cannot connect to database: check credentials! error: {0}".format(e))

    # ----

    def has_connection(self):

        return not self.failed_connection

    # ----

    def get_schema_names(self):

        if not self.has_connection():
            self.logger.error('get_schema_names: No connection!')
            return []

        if len(self.schema_names) > 0:
            return self.schema_names

        try:
            self.schema_names = self.inspector.get_schema_names()

            return self.schema_names

        except Exception as e:
            self.logger.warn("Database: could not get schemas! {0}".format(e))
            return []

    # ----

    def get_table_and_view_names(self, schema=None):

        if not self.has_connection():
            self.logger.error('get_schema_names: No connection!')
            return []

        if schema:
            selected_schemas = [schema]  # one specific schema
        else:
            selected_schemas = self.get_schema_names()  # or get all schemas in database

        for s in selected_schemas:
            self.logger.info("Get tables for schema: " + s)
            tables_and_views = self.inspector.get_table_names(schema=s) + self.inspector.get_view_names(
                schema=s)  # we treat tables and views the same here
            self.table_and_view_names += [s + '.' + n for n in tables_and_views]

        return self.table_and_view_names

    # ----

    def get_view_names(self, schema=None):

        if not self.has_connection():
            self.logger.error('get_view_names: No connection!')
            return []

        if schema:
            # one specific
            selected_schemas = [schema]
        else:
            # get all schemas in database
            selected_schemas = self.get_schema_names()

        for s in selected_schemas:
            self.logger.info("Get views for schema: " + s)
            self.view_names += [s + '.' + n for n in self.inspector.get_view_names(schema=s)]  # returns schema.viewname

        return self.view_names

    # ----

    def get_table(self, name=None, schema=None):

        """ Get Gutter Table instance to start working with it
        
            :return: Table -- Gutter Table instance 
        
        """

        if schema is None and name is None:
            self.logger.error('get_table failed. no name given!')
            return False

        if schema is None:
            schema, name = self.split_table_schema_name(name)

        if schema is None:
            # go out and look for schema by checking tables/views in database
            schema = self.find_schema_for_table(name)

        if schema is None:
            self.logger.warn("get_table has no schema to look in. probably take only public schema!")

        table = Table(self, name=name, schema=schema)  # supplies instance of database and name ( schema.name )

        if table is None:
            self.logger.error("No table found with name: {0}".format(name))

        return table

    # ----

    def split_table_schema_name(self, name):

        if '.' in name:
            s = name.split('.')
            return (s[0], s[1])  # schema table
        else:
            return (None, name)

    # ----

    def find_schema_for_table(self, table_name):

        """ Find schema name for table
        
            :param table_name: Name of table 
            :return str or None -- 
        
        """

        all_tables = self.get_table_and_view_names()  # returns list of schema.tablename

        try:
            first_schema_table = [schema_table for schema_table in all_tables if (table_name in schema_table)]

            if first_schema_table is []:
                self.logger.error("find_schema_for_table: Could not find schema for table: {0}".format(table_name))
            else:
                schema, table_name = self.split_table_schema_name(first_schema_table[0])

                return schema

        except Exception as e:
            self.logger.error("cannot find schema for table '{0}': {1}".format(name, e))
            return None

    # ----

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy import orm
from sqlalchemy.dialects.postgresql import JSONB

import datetime
import logging

db_obj = declarative_base()


class Pipeline(db_obj):
    __tablename__ = 'pipelines'
    __table_args__ = {"schema": "gutter"}  # hack to specify schema

    # define data structure
    # basic identifiers
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    type = Column(String(255))
    last_run = Column(DateTime)
    data_source = Column(JSONB())
    last_source_schema_def = Column(JSONB())
    primary_key = Column(String(255))  # can be manually set when not trivial
    map_source_target = Column(JSONB())
    last_automap = Column(JSONB())
    run_at = Column(JSONB())
    executing = Column(Boolean())
    max_duration = Column(Integer())
    last_duration = Column(Integer())

    # ----

    def __init__(self, name, type, data_source, map_source_target=None,
                 primary_key=None, last_run=None, last_source_schema_def=None, last_automap=None, run_at=None,
                 executing=None):

        self.id = None  # auto
        self.name = name
        self.type = type
        self.last_run = last_run
        self.data_source = data_source
        self.last_source_schema_def = last_source_schema_def
        self.primary_key = primary_key
        self.map_source_target = map_source_target
        self.last_automap = last_automap
        self.run_at = run_at
        self.executing = False

        # non_db properties
        self.source_schema_definition = None
        self.source_model = None
        self.source_table = None
        self.map = None

        self.create_logger()

    # ----

    @orm.reconstructor
    def init_on_load(self):

        """ Initiate when load from database
        
        """

        self.source_schema_definition = None
        self.source_model = None
        self.map = None

        self.create_logger()

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

    def __repr__(self):
        # string/unicode representation of object
        return """<Pipeline id='{0}', name='{1}', last_run='{2}',
                data_source='{3}', map_source_target='{4}', primary_key='{5}',
                type='{6}'>""".format(self.id, self.name, self.last_run, self.data_source,
                                      self.map_source_target, self.primary_key, self.type)

    # ----

    def to_dict(self):

        # todo: make this more robust?
        ALLOWED_PROPERTIES_OUTPUT = [unicode, str, bool, str, int, float, long, dict, list]

        # simple filter out internal SQLAlchemy keys
        d = {}
        for key in self.__dict__.keys():

            v = self.__dict__[key]

            if type(v) in ALLOWED_PROPERTIES_OUTPUT:
                if (type(v) is unicode or type(v) is str) and v is not None:
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
            self.logger.error('Cannot create_table: please supply engine!')
            return False

        try:
            DBObj.metadata.create_all(engine)

        except Exception as e:
            self.logger.error("Can't create table for pipeline: {0}".format(e))
            pass

    # ----

    def needs_doing(self, time_obj=None):

        """ Check if this pipeline needs to be executed
            
            :param time_obj:  { type: every|at, hour: 4 or minutes: 5 }
            :return: bool --
        
        """

        if time_obj is None:
            time_obj = self.run_at

        if time_obj is None:
            # self.logger.error("we need a time_obj to check if we need to do the job!")
            return False

        if self.executing == True:
            self.logger.warning("Pipeline is already executing!")
            return False

        now = datetime.datetime.now()

        if self.last_run is not None:
            if (now - self.last_run).total_seconds() / 60.0 < 1.0:
                # less than 1 minute ago that the pipeline was executed: don't do anything
                self.logger.info("This pipeline was run under a minute ago!")
                return False

        timing_type = time_obj.get('type')

        if timing_type == "at":  # by time
            try:
                hour = int(time_obj.get('hour'))

                if not hour:
                    # self.logger.error("please supply the hour you want to run the job")
                    return False

                if now.hour == hour and now.minute == 0:
                    # on rounded hour: <<hour>>:00h ex: 4:00
                    return True
            except Exception as e:
                # self.logger.error("failed to parse 'at' timing object!")
                return False

        elif timing_type == "every":  # every x minutes

            try:
                minutes = int(time_obj.get('minutes'))

                if not minutes:
                    # self.logger.error("please supply the minutes in the time_obj")
                    return False
                if self.run_at is None:
                    return True  # just run
                elif ((now - self.last_run).total_seconds() / 60.0) >= minutes:
                    return True

            except Exception as e:
                # self.logger.error("failed to parse 'every' timing object!")
                return False

        return False

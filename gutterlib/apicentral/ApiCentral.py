""" gutterlib.apicentral.ApiCentral

    Maintains and creates API Endpoints based on JSON schema definitions and data in GutterStore

    Entities:

        - ApiEndpoint - definition of endpoint in database
        - Schema definition - JSON schema definition

    General design of APIs:

        - based on Microsoft RestAPI (OData) design guidelines: https://github.com/Microsoft/api-guidelines
        - uses Flask-RestPlus
        - Human readible URL and understandable what unit of data you get
            
"""


from ..datastore.GutterStore import GutterStore

from .ApiEndPoint import ApiEndPoint
from .RequestHandler import RequestHandler
from .AccessController import AccessController
from ..datastore.GutterStoreError import GutterStoreError

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from flask_cors import CORS
from flask_jwt_extended.utils import get_jwt_identity, get_jwt_claims
from flask import json

from flask_restplus import Namespace, Resource, reqparse
from flask import request
from flask import jsonify

from flask_restplus import Api

import uuid
import logging
import datetime


class ApiCentral:

    def __init__(self, api_root=None, jwt_manager=None):
        
        # properties
        self.api_root = None # reference to flask restplus api object; set by create_end_points_on_api
        self.jwt_manager = None # reference to jwt manager in main scope
        self.db_engine = None
        self.db_session_maker = None
        self.db_session = None

        self.connection_data = None
        self.connection_string = None
        self.logger = None
        self.has_connection = False

        self.access_controller = None
        self.request_handler = None
        self.gutter_store = None

        # setup
        self.setup_logger()
        
        # handle input
        self.api_root = api_root
        if api_root is None:
            self.logger.warn("Please supply a reference to Restful Api instance from main api script!")
            
        self.jwt_manager = jwt_manager
        if jwt_manager is None:
            self.logger.warn("Please supply a reference to jtw manager instance from main api script!")
            

    # ----

    def __del__(self):
        # cleanup all connections
        if self.db_engine is not None:
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

    def connect(self, db_type=None, url=None, port=None, user=None, password=None, name=None, admin_username=None, admin_password=None):

        # connect to specific database that contains gutter data
        
        # some input checks
        if db_type is None:
            self.logger.error("Please supply a db_type like 'postgres', 'mysql', 'oracle'")
            return False
        if url is None:
            self.logger.error("Please supply a url of the database!")
            return False
        if port is None:
            self.logger.error("Please supply port for the database. Like 5432 for postgres")
            return False

        try:
            self.connection_string = '{0}://{1}:{2}@{3}:{4}/{5}'.format( db_type, user, password, url, port, name)

            self.connection_data = {
                'db_type': db_type, 'user': user, 'password': password,
                'url': url, 'port': port, 'name': name
            }

            self.db_engine = create_engine(self.connection_string, echo=False)
            self.db_session_maker = sessionmaker()
            self.db_session_maker.configure(bind=self.db_engine)
            self.db_session = self.db_session_maker()

            # if connection is successful this flag is set, otherwise Exception will happen
            self.db_engine.connect()  # db_engine is lazy, does not connect directly only if we do so
            self.has_connection = True

            self.logger.info("ApiCentral Connected to database") # is this really needed? only for checks. Do with GutterStore instance?
            self.check_and_create_database()

            self.create_admin_user(admin_username, admin_password)
            
            # make gutter store instance with connection data - request handlers use this one gutter store instance
            self.gutter_store = GutterStore()
            self.gutter_store.connect(**self.connection_data)
            self.request_handler = RequestHandler(api_central=self)
            self.request_handler.connect_to_gutter_store(self.gutter_store)
            
            return True

        except Exception as e:
            self.logger.error("Cannot connect to Gutter database! {0}".format(e))

            return False

    # ----

    def check_and_create_database(self):

        # we could have a fresh database: make sure all the needed tables are created
        ApiEndPoint().create_table(self.db_engine)
        AccessController().create_tables(self.db_engine)

    # ----

    def create_admin_user(self, username, password):

        if self.access_controller is None:
            self.access_controller = AccessController()
            self.access_controller.connect(**self.connection_data)

        self.access_controller.create_admin_user(username, password)

    def is_connected(self):

        return self.has_connection

    # ----

    def create_end_point(self, name=None, endpoint=None, unit=None,
                         gutter_table=None, schema_definition=None, active=False):

        """ places API endpoint object in database
        
        """
        
        if name is None or endpoint is None or gutter_table is None or schema_definition is None:
            self.logger.error("Cannot create endpoint: missing input!")
            return False

        if not self.is_connected():
            self.logger.error("Cannot create endpoint: no connection to Gutter database")
            return False

        new_endpoint = ApiEndPoint(name, endpoint, unit,
                                   gutter_table, schema_definition, active)

        try:
            self.db_session.add(new_endpoint)
            self.db_session.commit()
            return new_endpoint
        except Exception as e:
            self.logger.error("Error creating endpoint with name: '{0}'. Error: {1}".format(name, e))
            return None

    # ----

    def get_end_point(self, name=None):

        """ Get end point data object from database
        
        """

        if name is None:
            self.logger.error("get_end_point: Please supply a name!")
            return None

        try:
            end_point = self.db_session.query(ApiEndPoint).filter(ApiEndPoint.name == name).first()
            # can be None is no exists with that name
            return end_point
        except Exception as e:
            self.logger.error(e)
            return None

    # ----

    def update_end_point(self, name, **kwargs):

        """ Updates end point object
        
        """

        end_point = self.get_end_point(name)

        if end_point is None:
            self.logger.error("No endpoint found with name {0}".format(name))
            return False

        for prop, value in kwargs.iteritems():
            if value is not None:
                if hasattr(end_point, prop):
                    setattr(end_point, prop, value)

        self.db_session.commit()  # make final

        return True

    # ----

    def get_end_points(self):

        try:
            end_points = self.db_session.query(ApiEndPoint).filter(ApiEndPoint.active == True).all()
            return end_points  # can be None is no exists with that name
        except Exception as e:
            self.logger.error(e)
            return []

    #### Dynamic API generation #### 
    
    # using namespace strategy: http://flask_restplus.readthedocs.io/en/stable/scaling.html

    def get_api_namespace_for_end_point(self, api_end_point):
        
        """ Generate and return an RestPlus API instance
        
        :param ApiEndPoint: a Gutter ApiEndPoint object
        :return: Restplus API instance 
        
        """
        
        # IMPORTANT api.abort generates ugly errors - phase out
        # get Restplus API object as namespace to combine with main api endpoint

        if not isinstance(api_end_point, ApiEndPoint):
            end_point_definition = self.get_end_point(api_end_point)
        else:
            end_point_definition = api_end_point

        if end_point_definition is None:
            self.logger.error("No endpoint found with name {0}".format(api_end_point))
            return None

        # start generating endpoint
        
        from flask_jwt_extended import jwt_required # IMPORTANT: locally imported

        api = Namespace(end_point_definition.endpoint, description='---- No description yet ----') # TODO: get description from somewhere? 
        api_model = self.generate_restplus_api_marshall_model(end_point_definition, api) # 

        # NOTE: central request handler is initialized first, we only supply the specific api_end_point object in every call
        request_handler = self.request_handler

        # dynamic decorator ( see: https://stackoverflow.com/questions/20850571/decorate_a_function_if_condition_is_True)
        def decorate_conditional(condition, decorator):
            return decorator if condition else lambda x: x

        @api.route('')  # <== NOTE: don't use / otherwise the trailing slash will be added to endpoints like waarnemingen/
        class List(Resource):
            @decorate_conditional(end_point_definition.anonymous_access != True, jwt_required) # NOTE: this controls if the end_point is accessable without JWT token
            @api.doc('---- list description TODO ----')
            # @api.marshal_with(api_model) # NOTE: disabled for geojson output: can we add this
            def get(self):
                parser = reqparse.RequestParser()
                parser.add_argument('$filter', type=str, help="Filter the data. Ex: 'column_name' eq 1000'")
                parser.add_argument('$top', type=int, help='Limit results to a certain number')
                parser.add_argument('$skip', type=int, help='Skip certain results')
                parser.add_argument('$orderBy', type=str, help='Order by column')
                parser.add_argument('$format', type=str, help='Special output formats besides json')

                args = parser.parse_args()

                # return list of dicts / or geojson
                data_rows = request_handler.get_data_list(api_end_point=end_point_definition, request_data=args)

                return data_rows

            @decorate_conditional(end_point_definition.anonymous_access is not True, jwt_required)
            @api.doc('Upload a {0}'.format(end_point_definition.unit))
            @api.expect(api_model)
            # @api.marshal_with(api_model, code=201)  # disable for now: things go wrong with nested arrays in objects
            # return getattr(obj, key, default) type_error: getattr(): attribute name must be string 
            # besides: data is checked with schema checked by hand
            def post(self):
                if request.get_json():
                    args = request.get_json()
                else:
                    args = request.values.to_dict()

                new_row = request_handler.insert_data(api_end_point=end_point_definition, user=get_jwt_identity(), data=args)

                if isinstance(new_row, GutterStoreError):
                    return { "status" : "error", "message" : new_row.msg }, new_row.status_code or 500
                else:
                    return new_row.get_data_dict(), 201
                    

        @api.route('/<id>')
        @api.param('id', 'Data object unique identifier as string')
        @api.response(404, 'No valid id given!')
        class DataRow(Resource):
            @decorate_conditional(end_point_definition.anonymous_access is not True, jwt_required)
            @api.doc('get a {0}'.format(end_point_definition.unit))
            # @api.marshal_with(api_model)
            def get(self, id_):
                data_row = request_handler.get_data_by_id(api_end_point=end_point_definition, id_=id_)
                if data_row is None:
                    api.abort(404, "Unique id '{0}' not found!".format(id_))
                return data_row.get_data_dict()

            @decorate_conditional(end_point_definition.anonymous_access is not True, jwt_required)
            @api.doc('update a {0}'.format(end_point_definition.unit))
            # @api.expect(api_model) # NOTE: on nested objects with numbers things go wrong!!
            def put(self, id):
                # TODO: check id?

                data = request.get_json() # take json payload as input

                if data is None:
                    api.abort(404, "Please supply a JSON payload".format(id_))

                data['id'] = id # make sure data contains the id

                data_row = request_handler.update_data(api_end_point=end_point_definition, data=data)
                if data_row is None:
                    api.abort(404, "Unique id '{0}' not found!".format(id))
                return data_row.get_data_dict()

            @decorate_conditional(end_point_definition.anonymous_access is not True, jwt_required)
            @api.doc('Delete a {0}'.format(end_point_definition.unit))
            def delete(self, id_):
                # True or False
                result = request_handler.delete_data(id_)

                if result is True:
                    return {'message': "row with id '{0}' successfully deleted".format(id_)}
                else:
                    return {'message': "no row deleted. check if id '{0}' exists".format(id_)}, 400

        return api

    # ----

    def generate_restplus_api_marshall_model(self, api_end_point_obj, api_obj):

        """ Restplus uses this to check input/output and generate documentation
        
        See: https://flask-restplus.readthedocs.io/en/stable/marshalling.html
        
        """  

        if api_end_point_obj is None:
            self.logger.error("generate_restplus_api_marshall_model: no api_end_point_obj given!")
            return None

        if api_end_point_obj.schema_definition is None:
            self.logger.error("This API endpoint definition has no schema!")
            return None

        from flask_restplus import fields

        api_model_fields = {}

        for prop_name, prop_def in api_end_point_obj.schema_definition['properties'].items():

            prop_title = prop_def.get('title') or '... (TODO: see schema def in endpoint)'
            prop_description = prop_def.get('description') or '...'
            prop_read_only = prop_def.get('read_only', False)

            if prop_def.get('type') == 'number':
                api_model_fields[prop_name] = fields.Float(
                    required=False, title=prop_title,
                    description=prop_description, readonly=prop_read_only)

            elif prop_def.get('type') == 'array':
                api_model_fields[prop_name] = fields.List(
                    fields.String, title=prop_title,
                    description=prop_description, readonly=prop_read_only)

            elif prop_def.get('type') == 'object':
                api_model_fields[prop_name] = fields.Raw(
                    required=False, title=prop_title,
                    description=prop_description,
                    readonly=prop_read_only)  # hack: see: https://flask_restplus.readthedocs.io/en/stable/_modules/flask_restplus/fields.html

            elif prop_def.get('type') == 'string':
                api_model_fields[prop_name] = fields.String(
                    required=False, title=prop_title,
                    description=prop_description, readonly=prop_read_only)

            elif prop_def.get('type') == 'integer':
                api_model_fields[prop_name] = fields.Integer(
                    required=False, title=prop_title,
                    description=prop_description, readonly=prop_read_only)

            else:
                self.logger.error(
                    "ERROR: creating mashaling model for REST plus for table '{0}'. "
                    "unknown type in schema definition: {1}".format(
                        api_end_point_obj.gutter_table, prop_def.get('type')))

        # add id as standard
        api_model_fields['id'] = fields.String(required=True, description='unique id')

        api_model = api_obj.model(api_end_point_obj.unit, api_model_fields)

        return api_model

    # ----

    def create_end_points_on_api(self):

        if self.api_root is None:
            self.logger.error("Please supply Restplus API root instance before we can set endpoint!")
            return False

        # !!!! IMPORTANT: we don't do anything with jwt_manager now !!!!
        if self.jwt_manager is None:
            self.logger.warning("Please supply JWTManager object")

        end_points = self.get_end_points()

        if len(end_points) == 0:
            self.logger.error("No end points to create API")
            return False

        for end_point in end_points:
            api_namespace = self.get_api_namespace_for_end_point(end_point)
            self.api_root.add_namespace(api_namespace, path='/' + end_point.endpoint)
            self.logger.info("Created API '{0}' on endpoint '{1}'".format(end_point.name, end_point.endpoint))

        return True

    # ----

    def check_for_new_end_points(self):

        """ TODO: update strategy for new and updated endpoints
        
        """

        pass

    # ----

    def create_api_as_service(self, schema_definition=None, endpoint_props={}):
        
        """ Create an endpoint just from a JSON Schema 
        
        :return: ApiEndPoint instance
        
        """

        if schema_definition is None:
            self.logger.error("Please supply a JSON schema to set up a API as a service!")
            return False

        # TODO: validate JSON schema input

        api_title = schema_definition.get('title')
        if api_title is None:
            self.logger.error("No title in JSON schema!")
            return False

        # check for existing endpoint with this name
        if self.get_end_point(api_title) is not None:
            self.logger.error("An API endpoint with name {0} already exists".format(api_title))
            return False

        # set up table in gutter_store
        gutter_store = GutterStore()
        gutter_store.connect(**self.connection_data)

        if not gutter_store.is_connected():
            self.logger.error("Cannot create indices: failed setup of GutterStore")
            return False

        if gutter_store.table_is_present(api_title):
            self.logger.error("There is a table named {0} already. Please check!".format(api_title))
            return False

        # create tables in GutterStore
        try:
            StorageModel = gutter_store.get_storage_model(api_title)
            dummy_row = StorageModel().create_table(self.db_engine)
            HistoryModel = gutter_store.get_history_model(api_title)
            history_dummy_row = HistoryModel().create_table(self.db_engine)
            self.logger.info("Setup table in GutterStore: {0}".format(api_title))
        except Exception as e:
            self.logger.error("Error setting GutterStore structure: {0}".format(e))
            return False

        # insert test value
        """
        test_row = StorageModel()
        test_row.data = {'test': '1'}
        gutter_store.add_rows([test_row])
        gutter_store.commit()
        """

        # save endpoint
        new_endpoint = self.create_end_point(name=api_title, endpoint=api_title, unit=None,gutter_table=api_title, schema_definition=schema_definition, active=True)
        
        # set extra properties
        try:
            for key,val in endpoint_props.items():
                if hasattr(new_endpoint, key):
                    setattr(new_endpoint, key, val)
        except Exception as e:
            self.logger.error(e)

        # place indices
        self.create_indices_on_endpoint(new_endpoint)

        self.logger.info("Create API with storage, indices on endpoint {0}".format(schema_definition['title']))
        
        # reload API
        self.request_reload_api()
        
        return new_endpoint

    # ----

    def add_access_control_resources_to_api(self):

        """ Adds authorization logic (/login, /refresh etc) to API
        
        """

        if self.access_controller is None:
            self.access_controller = AccessController()
            self.access_controller.connect(**self.connection_data)

        if self.jwt_manager is None:
            self.logger.warn("No jwt_manager given!")

        if not self.access_controller.has_connection:
            self.logger.error("AccessController has no connection to database!")
            return False

        # set create login endpoints and 
        self.access_controller.add_access_control_resources_to_api(self.api_root, self.jwt_manager)
        
    # ----
        
    def request_reload_api(self):
        
        """ Rebuild all data endpoints
        
        """
        # self.reset_api() # not needed anymore: we'll use Flask reset
        self.api_root.app.config['NEEDS_RELOAD'] = True # request for reload
        
        return { "stats" : "succes", "message" : "refreshed API!" }
        
    #### admin endpoint ####
    
    def set_admin_api(self):
        
        """ Create the admin functionality on endpoint /admin/
        
        Only admin users ( User.admin = True ) can do these actions like:
        - Maintaining endpoints ( new, delete, update, get )
        - Make other users admin
        - TODO: refresh endpoints on API
        
        """
        
        from flask_jwt_extended import jwt_required # IMPORTANT NEED TO BE LOCALLY IMPORTED: jwt_refresh_token_required needed for checks on old tokens
        from flask_jwt_extended import get_jwt_identity
        from flask_jwt_extended import get_jwt_claims
        
        
        this_api_central = self
        # TODO: make this into a decorator?
        def check_admin():
            is_admin = get_jwt_claims().get('admin', False)
            return is_admin
        
        #### setup refresh endpoint ####
        
        app = self.api_root.app
        @app.route('/admin/reload')
        @jwt_required
        def reload_app():
            app.config['NEEDS_RELOAD'] = True
            
            if not check_admin():
                return { "status" : "error", "message" : "You can only use these admin functions as admin!" }, 401
            
            result = this_api_central.request_reload_api()
            
            return jsonify(result)
            
        @app.route("/admin/started")
        def last_restarted():
            return "last started: {0}".format( app.config['LAST_RELOADED'])

        #### /admin/users ####

        user_namespace = Namespace('users', description='Gutter users')
        self.api_root.add_namespace(user_namespace, path='/admin/users')

        @user_namespace.route('')
        class UserList(Resource):
            @jwt_required
            def post(self):
                if not check_admin():
                    return {"status": "error", "message": "You can only use these admin functions as admin!"}, 401

                payload = request.get_json()

                if this_api_central.access_controller is None:
                    this_api_central.access_controller = AccessController()
                    this_api_central.access_controller.connect(**self.connection_string)

                new_user = this_api_central.access_controller.create_user(
                    email=payload.get("email"),
                    password=payload.get("password"))

                return {
                           "status": "success",
                           "message": "Successfully created user: {0}".format(new_user.email)
                       }, 200



        #### /admin/endpoints ####
        
        endpoint_model = ApiEndPoint().to_restplus_marshall_fields() # restplus marshalling model for docs and checks
        endpoint_namespace = Namespace('endpoints', description='Admin endpoints')
        self.api_root.add_namespace(endpoint_namespace, path='/admin/endpoints')
            
        @endpoint_namespace.route('')
        class EndPointList(Resource):
            #@endpoint_namespace.marshal_with(endpoint_model) # TODO: fix marshalling
            @jwt_required
            def get(self):
                
                if not check_admin():
                    return { "status" : "error", "message" : "You can only use these admin functions as admin!" }, 401
                
                endpoints = [ e.to_dict() for e in this_api_central.get_end_points() ] # automatically is converted to json output
                return endpoints # returns an empty list if no endpoints are found
            
            # ----
        
            @jwt_required
            #@endpoint_namespace.expect(endpoint_model) # Don't use it here: allow for both JSON Schema or EndPoint object
            def post(self):
                """ Either POST an JSON SCHEMA or EndPoint object to create an endpoint
                
                """
                
                if not check_admin():
                    return { "status" : "error", "message" : "You can only use the /admin endpoints as admin!" }, 401
                
                payload = request.get_json() # get JSON payload
                endpoint_props = {}
                
                if payload.get('title') and payload.get('properties'):
                    # is probably a JSON schema
                    schema = payload
                elif payload.get('schema_definition'):
                    # EndPoint object
                    schema = payload.get('schema_definition')
                    endpoint_props['anonymous_access'] = payload.get('anonymous_access') # NOTE we ignore the rest of the input ( name, endpoint, unit, gutter_table, active ) except anonymous_access and unit
                    endpoint_props['unit'] = payload.get('unit')
                # simple check
                if type(schema) is not dict:
                    return { "status" : "error", "message" : "Bad input. Please supply a EndPoint model or a simple JSON Schema!"}, 422
                else:
                    new_endpoint = this_api_central.create_api_as_service(schema_definition=schema, endpoint_props=endpoint_props)
                    if new_endpoint is False:
                        return { "status" : "error", "message" : "Cannot make your endpoint. Check if the name is not already taken!" }, 422
                          
                    return { "status" : "succes", "message" : "Succesfully created EndPoint. Your endpoint is here: <<YOUR_URL>>/{0}".format(new_endpoint.endpoint) }, 200
                
          
        @endpoint_namespace.route('/<name>')
        class EndPoint(Resource):
            #@endpoint_namespace.marshal_with(endpoint_model) # TODO: fix marshalling
            @jwt_required
            def get(self,name):
                
                if not check_admin():
                    return { "status" : "error", "message" : "You can only use the /admin endpoints as admin!" }, 401
                                
                endpoint = this_api_central.get_end_point(name)
                if endpoint:
                    return endpoint.to_dict()
                
            # ----
                
            @jwt_required
            def delete(self,name):
                
                if not check_admin():
                    return { "status" : "error", "message" : "You can only use the /admin endpoints as admin!" }, 401
                
                endpoint = this_api_central.get_end_point(name)
                
                if endpoint is None:
                    return { "status" : "error", "message" : "No endpoint with name '{0}' to delete!".format(name) }, 404
                else:
                    # remove endpoint with all data!
                    pass
                

    #### some extra api manipulations ####

    def set_cors(self, app=None):

        if app is None:
            self.logger.error("No app given to set CORS")
            return False

        try:
            CORS(app)
        except Exception as e:
            self.logger.error("Cannot set CORS: {0}".format(e))

    # ----

    def set_upload_endpoint(self, flask_app=None):
        
        """ Extend app to allow for uploads
        
        """

        # NOTE: move these to top
        from flask import request, redirect, send_from_directory
        import os

        ALLOWED_EXTENSIONS = ['jpg']
        UPLOAD_FOLDER = './uploads'
        MAX_SIZE = 10

        if flask_app is None:
            self.logger.error("No api_root given to set upload endpoint")
            return False

        def allowed_file(filename):
            return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

        # set upload route: plainly taken from: http://flask.pocoo.org/docs/0.12/patterns/fileuploads/

        # set maximum upload size
        flask_app.config['MAX_CONTENT_LENGTH'] = MAX_SIZE * 1024 * 1024  # 10 mb

        @flask_app.route('/upload', methods=['GET', 'POST'])
        def upload_file():
            if request.method == 'POST':
                # check if the post request has the file part
                if 'file' not in request.files:
                    # return to this url
                    return redirect(request.url)

                file_ = request.files['file']

                # if user does not select file, browser also
                # submit a empty part without filename
                if file_.filename == '':
                    return redirect(request.url)
                if file_ and allowed_file(file_.filename):
                    # filename = secure_filename(file_.filename)
                    file_name_id = str(uuid.uuid4())
                    file_extension = file_.filename.rsplit('.', 1)[1].lower()
                    file_name = file_name_id + '.' + file_extension
                    file_.save(os.path.join(UPLOAD_FOLDER, file_name))
                    # return id
                    return file_name

            return '''
            <!doctype html>
            <title>upload file test</title>
            <p>upload file test</p>
            <form method="post" enctype="multipart/form_data">
              <p><input type="file" name="file"><input type="submit" value="Upload"></form>
            '''

        # do this for testing purposes ( maybe serve static files directly? )
        # TODO: make this work on file system!
        @flask_app.route('/uploads/<file_id_with_ext>', methods=['GET', 'POST'])
        def get_uploaded_file(file_id_with_ext):
            # check id
            try:
                file_name = file_id_with_ext.split('.')[0]
                validated_id = uuid.UUID(file_name, version=4)
            except ValueError:
                return 'wrong image id'

            # serve it
            return send_from_directory(UPLOAD_FOLDER, file_id_with_ext, as_attachment=False)
        
    # ----
    
    def set_postman_endpoint(self, flask_app=None):
        
        """ Export API as Postman collection
        
        See: https://flask-restplus.readthedocs.io/en/stable/postman.html
        
        """
        
        if self.api_root is None:
            self.logger.error("No api_root given to set upload endpoint")
            return False
        
        if flask_app is None:
            self.logger.error("No flasp app given to set upload endpoint")
            return False
        
        @flask_app.route('/postman', methods=['GET', 'POST'])
        def postman():
            urlvars = False  # Build query strings in URLs
            swagger = True  # Export Swagger specifications
            data = self.api_root.as_postman(urlvars=urlvars, swagger=swagger)
            return json.dumps(data)

    #### utils for endpoints ###

    def create_indices_on_endpoint(self, name_or_obj=None):

        if isinstance(name_or_obj, ApiEndPoint):
            end_point = name_or_obj
        else:
            end_point = self.get_end_point(name_or_obj)

        if end_point is None:
            self.logger.error("No endpoint found with name {0}".format(name_or_obj))
            return False

        gutter_store = GutterStore()
        gutter_store.connect(**self.connection_data)

        if not gutter_store.is_connected():
            self.logger.error("Cannot create indices: failed setup of GutterStore")
            return False

        gutter_store.create_indices(
            table_name=end_point.gutter_table,
            schema_definition=end_point.schema_definition)

    # ----

    def drop_indices_on_endpoint(self, name=None):

        end_point = self.get_end_point(name)

        if end_point is None:
            self.logger.error("No endpoint found with name {0}".format(name))
            return False

        gutter_store = GutterStore()
        gutter_store.connect(**self.connection_data)

        if not gutter_store.is_connected():
            self.logger.error("Cannot create indices: failed setup of GutterStore")
            return False

        gutter_store.drop_indices(
            table_name=end_point.gutter_table,
            schema_definition=end_point.schema_definition)

    # ----

    def create_view_for_end_point(self, name=None):

        end_point = self.get_end_point(name)

        if end_point is None:
            self.logger.error("No endpoint found with name {0}".format(name))
            return False

        gutter_store = GutterStore()
        gutter_store.connect(**self.connection_data)

        if not gutter_store.is_connected():
            self.logger.error("Cannot create view: failed setup of GutterStore")
            return False

        gutter_store.create_data_view(
            table_name=end_point.gutter_table,
            schema_definition=end_point.schema_definition)

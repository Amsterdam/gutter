import os

from flask import Flask
from flask import Blueprint
from flask_restplus import Api
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_jwt_extended import JWTManager
from werkzeug.serving import run_simple

import datetime
import logging

from gutterlib.apicentral.ApiCentral import ApiCentral

""" SETTINGS 

Please make sure these settings are populated with environment variables:
* local in Eclipse: window => preferences => PyDev => Intepretors => Environment
* in Docker deployment they are set in docker-compose.yml or .env file

"""

GUTTER_DATABASE = {'db_type': os.environ.get('GUTTER_DB_TYPE'),
                   'url': os.environ.get('GUTTER_DB_URL'),
                   'port': os.environ.get('GUTTER_DB_PORT'),
                   'user': os.environ.get('GUTTER_DB_USER'),
                   'password': os.environ.get('GUTTER_DB_PASSWORD'),
                   'name': os.environ.get('GUTTER_DB_NAME'),
                   'admin_username': os.environ.get('GUTTER_ADMIN_USER'),
                   'admin_password': os.environ.get('GUTTER_ADMIN_PASSWORD')}

API_DESCRIPTION = """
    Gutter API maintains various (dynamic) endpoints for sets.
    Most require getting a JWT token at /login. Use your username/password to get one.
    All list endpoints have meta options $filter, $top, $skip
    - $filter: {{column_name}} {{logic_operator}} {{value|'value with spaces'}}. 
        Logic operators: eq (=), ge (&gt;=), gt (&gt;), lt (&lt;), le (&lt;=)
        Ex: ?$filter=datum_van gt 2018-08-01
        Ex with multiple conditions: $filter=datum_van gt 2018-08-01 and werkzaamheden eq festival

    - $top=N: select only the first N objects
    - $skip=N: skip the first N objects
    - $orderBy={{column_name}}: order results by column name. Add 'desc' to sort in descending order: waarnemingen_real?$orderBy=created_at desc&$top=10
    - [EXPERIMENTAL] $format=geojson: output response as GeoJson for GIS applications

    Uploads of images (tmp):
    POST (multipart/form-data) /upload - file=[[IMG.jpg]]
        returns [[id]].jpg
        image at /uploads/[[id]].jpg
"""
    
#### END SETTINGS ####

# reloading method: https://gist.github.com/nguyenkims/ff0c0c52b6a15ddd16832c562f2cae1d

def init_app(app):

    # sets config for logging (which is a singleton): https://docs.python.org/2/library/logging.html#logging.basicConfig
    # - fixes 'no handler found' when ran using Docker
    # - fixes duplicate output across application
    # - fixes having to set logger and handler in every 'base'-class e.g. ApiCentral, GutterStore ...
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)-4s %(message)s')

    with open('images/ascii_logo.txt', 'r') as f:
        logo = f.read()
        app.logger.info(logo)


    app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY') or 'PLEASE_FILL_IN_A_SECRET_KEY_IN_ENVIRONMENT_VARIABLES!!'
    app.config['JWT_SECRET_KEY'] = os.environ.get('FLASK_JWT_SECRET_KEY') or 'PLEASE_FILL_IN_A_SECRET_KEY_IN_ENVIRONMENT_VARIABLES!!'
    
    app.config['JWT_BLACKLIST_ENABLED'] = True
    app.config['JWT_BLACKLIST_TOKEN_CHECKS'] = ['access', 'refresh']
    
    # use config dictionary to set some flags ( HACKY )
    app.config['NEEDS_RELOAD'] = False
    app.config['LAST_RELOADED'] = datetime.datetime.now()
    
    app.wsgi_app = ProxyFix(app.wsgi_app)
    
    api = Api(app=app, title='Gutter API', version='1.0', description=API_DESCRIPTION)
    
    """ PATCH to force https reference to swagger.json
    """
    
    # =========== #
    
    # from flask import url_for
    # @property
    # def specs_url(self):
    #         """Monkey patch for HTTPS"""
    #         return url_for(self.endpoint('specs'), _external=True, _scheme='https')
    # Api.specs_url = specs_url
    
    #### END PATCH ####
    
    jwt = JWTManager(app)
    api_central = ApiCentral(api_root=api, jwt_manager=jwt)
    api_central.set_cors(app)
    api_central.set_upload_endpoint(app)
    succes = api_central.connect(**GUTTER_DATABASE)
    if succes:
        api_central.add_access_control_resources_to_api() # setup login endpoints
        api_central.create_end_points_on_api() # setup data API endpoints
        api_central.set_admin_api()
        api_central.set_postman_endpoint(app) # postman export endpoint
        
    else: 
        print(" * Gutter: No API endpoint created. Check output of ApiCentral")
        
# ----

def get_app():
    
    print("* Created Flask app for dynamic reloading")
    
    app = Flask(__name__)

    init_app(app)
    
    #### Refresh debug #### 
    """
    started_at = datetime.datetime.now()
    @app.route('/reload')
    def reload():
        #app.config['NEEDS_RELOAD'] = True
        global to_reload
        to_reload = True
        return "reloaded"
    
    # to make sure of the new app instance
    @app.route("/last_started")
    def index():
        return "time {0}".format( started_at )
    """

    return app

# ----

class AppReloader(object):
    # this extends the get_app function
    def __init__(self, create_app):
        self.create_app = create_app
        self.app = create_app()

    def get_application(self):
        #global to_reload
        to_reload = self.app.config['NEEDS_RELOAD']
        
        if to_reload:
            print ("* Refresh Flask app at '{0}'".format(datetime.datetime.now()))
            self.app = self.create_app()
            self.app.config['NEEDS_RELOAD'] = False
            self.app.config['LAST_RELOADED'] = datetime.datetime.now()

        return self.app

    def __call__(self, environ, start_response):
        app = self.get_application()
        return app(environ, start_response)



app = AppReloader(get_app)

# test server
if __name__ == '__main__':
    run_simple('localhost', 5000, app, use_reloader=True, use_debugger=True, use_evalex=True)

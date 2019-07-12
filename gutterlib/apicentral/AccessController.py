"""
    
    gutterlib.apicentral.AccessController
    
    - controls access to APIs

"""

from .AccessControllerModels import ApiUser, ApiRevokedToken

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from flask_jwt_extended import get_jwt_identity

import logging


class AccessController:

    # ----

    def __init__(self):

        # properties
        self.db_engine = None
        self.db_session_maker = None
        self.db_session = None

        self.connection_data = {}
        self.connection_string = None
        self.logger = None
        self.has_connection = False

        self.setup_logger()

    # ----

    def __del__(self):
        # cleanup all connections
        try:
            if self.db_engine:
                self.db_engine.dispose()
        except Exception as e:
            self.logger.error(e)

    # ----

    def connect(self, db_type, url, port, user, password, name):
        
        try:
            self.connection_string = '{0}://{1}:{2}@{3}:{4}/{5}'.format(
                db_type, user, password,
                url, port, name)

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

            self.logger.info("AccessController instance connected to database")

            return True

        except Exception as e:
            self.logger.error("Cannot connect to Gutter database! {0}".format(e))

            return False

    # ----

    def setup_logger(self):

        self.logger = logging.getLogger(__name__)

        if not self.logger.handlers:
            logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)-4s %(message)s')

    # ----

    def create_tables(self, engine=None):

        if not engine and not self.has_connection:
            self.logger.error("Please connect to database first!")
            return False

        if engine is None and self.db_engine is not None:
            engine = self.db_engine

        try:
            ApiUser().create_table(engine)
            ApiRevokedToken().create_table(engine)
            return True
        except Exception as e:
            self.logger.error(e)
            return False

    # ----

    def get_user(self, email=None):

        if email is None:
            self.logger.error("Supply an email to get a user!")
            return False

        try:
            user = self.db_session.query(ApiUser).filter(ApiUser.email == email).first()
            return user
        except Exception as e:
            self.logger.error("No user with email {0}".format(email))
            return None

    # ----

    def get_revoked_token(self, jti=None):

        if jti is None:
            self.logger.error("Supply jti!")
            return False

        revoked_token = self.db_session.query(ApiRevokedToken).query(ApiRevokedToken.jti == jti).first()

        return revoked_token

    # ----

    def add_revoked_token(self, jti=None):

        if jti is None:
            self.logger.error("Supply jti!")
            return False

        try:
            new_revoked_token = ApiRevokedToken(jti=jti)
            self.db_session.add(new_revoked_token)
            self.db_session.commit()

            self.logger.info("Created Revoked Token!")

            return new_revoked_token

        except Exception as e:
            self.logger.error(e)

    # ----

    def create_user(self, email=None, password=None, role=None, allowed_apps=None, rights_by_app=None):

        if email is None and password is None:
            self.logger.error("Cannot create User without email and password")
            return None

        if self.get_user(email) is not None:
            self.logger.error("There is already a user with email address {0}".format(email))
            return None

        new_user = ApiUser(email=email, password=password,
                           allowed_apps=allowed_apps, rights_by_app=rights_by_app)

        try:
            self.db_session.add(new_user)
            self.db_session.commit()

            self.logger.info("User with email {0} created".format(new_user.email))

            return new_user

        except Exception as e:
            self.logger.error("Error create new user: {0}".format(e))
            return False

    # ----

    def create_admin_user(self, username, password):

        if self.get_user(username) is not None:
            # Not printing anything for security reasons!
            # self.logger.error("Error when adding admin user")
            return None

        admin_user = ApiUser(email=username, password=password, admin=True)

        try:
            self.db_session.add(admin_user)
            self.db_session.commit()

            self.logger.info("Admin user created")
            return True

        except Exception as e:
            self.logger.error("Error when creating admin user: {0}".format(e))
            return False

    def is_revoked_token(self, jti=None):

        if jti is None:
            self.logger.error("Please supply a JST hash!")
            return False

        revoked_token = self.db_session.query(ApiRevokedToken).filter(ApiRevokedToken.jti == jti).first()

        if revoked_token:
            return True

        return False

    # ----

    def add_access_control_resources_to_api(self, flask_api=None, jwt_manager=None):

        # add endpoint for user registration, user verification and jwt generation
        # jwt is to access jtw functions

        if flask_api is None or jwt_manager is None:
            self.logger.error("Please supply Flask API object to add access control API points!")
            return False

        # TODO: these imports somewhere else?
        from flask_restplus import Resource, reqparse
        from flask import request
        from flask_jwt_extended import create_access_token, create_refresh_token, jwt_required, \
            jwt_refresh_token_required, get_raw_jwt, decode_token

        parser = reqparse.RequestParser()
        parser.add_argument('username', help='This field cannot be blank', required=True)
        parser.add_argument('password', help='This field cannot be blank', required=True)

        access_controller = self
        
        # add admin flag into jwt on creation of jwt token: see https://flask-jwt-extended.readthedocs.io/en/latest/add_custom_data_claims.html
        @jwt_manager.user_claims_loader
        def add_claims_to_access_token(identity):
            user = access_controller.get_user(email=identity)
            
            if user:
                return { 'admin': user.admin }
            else:
                return { 'admin' : False }

        # revoked tokens
        @jwt_manager.token_in_blacklist_loader
        def check_if_token_in_blacklist(decrypted_token):
            jti = decrypted_token['jti']
            return access_controller.is_revoked_token(jti)

        # IMPORTANT: disabled user registration here ( taken from: https://github.com/oleg_agapov/flask_jwt_auth )
        """
        class UserRegistration(Resource):
            def post(self):
                data = parser.parse_args()
                user_name = data.get('username')
                user_password = data.get('password')
                
                if user_name is None and user_password is None:
                    return {'message': 'user_registration needs username and password input'}, 400 # note return code
                    
                new_user = access_controller.create_user(email=user_name, password=user_password)
                
                if not new_user:
                    return {'message': 'user {0} already exists'.format(user_name)}, 400
                
                try:
                    access_token = create_access_token(identity = new_user.email)
                    refresh_token = create_refresh_token(identity = new_user.email)
                    return {
                        'message': 'user {0} was created'.format(new_user.email),
                        'access_token': access_token,
                        'refresh_token': refresh_token
                    }
                except:
                    return {'message': 'something went wrong creating a new user!'}, 500
        
        """

        # ----

        class UserLogin(Resource):
            def post(self):
                data = parser.parse_args()
                user_name = data.get('username')
                user_password = data.get('password')

                current_user = access_controller.get_user(email=user_name)

                if not current_user:
                    return {'message': 'user {0} doesn\'t exist'.format(user_name)}, 400

                if current_user.verify_password(user_password):
                    access_token = create_access_token(identity=user_name) # NOTE: also encode admin=True/False flag
                    refresh_token = create_refresh_token(identity=user_name)
                    return {
                        'message': 'logged in as {0}'.format(current_user.email),
                        'access_token': access_token,
                        'refresh_token': refresh_token
                    }
                else:
                    return {'message': 'wrong credentials'}, 401

        # ----

        class UserLogoutAccess(Resource):
            @jwt_required
            def post(self):
                jti = get_raw_jwt()['jti']
                try:
                    revoked_token = access_controller.create_revoked_token(jti=jti)
                    return {'message': 'Access token has been revoked'}
                except:
                    return {'message': 'Something went wrong'}, 500

        # ----

        class UserLogoutRefresh(Resource):
            @jwt_refresh_token_required
            def post(self):
                jti = get_raw_jwt()['jti']
                try:
                    revoked_token = access_controller.create_revoked_token(jti=jti)
                    return {'message': 'Access token has been revoked'}
                except:
                    return {'message': 'Something went wrong'}, 500

        # ----

        class TokenRefresh(Resource):
            # @jwt_refresh_token_required
            def post(self):

                # do a hack specially to also go along the o_auth2 spec 
                post_content_type = request.headers['Content-Type']

                refresh_token = None

                if post_content_type == 'application/json':
                    bearer_token_header = request.headers.get('Authorization')
                    if bearer_token_header:
                        refresh_token = bearer_token_header.replace("Bearer ", "")
                else:  # other post methods ( x_www_form_urlencoded and normal form_data )
                    refresh_token = request.values.get('refresh_token')

                if refresh_token is not None:
                    jwt_dict = decode_token(refresh_token)
                    # email
                    current_user = jwt_dict.get('identity')
                else:
                    return {
                               'message': 'Please supply refresh token: '
                                          'either in authorization header or post variable refresh_token!'}, 400

                if current_user is None:
                    return {
                               'message': 'Could not refresh token: no user found! '
                                          'wrong jtw in post var refresh_token or bearer token header!'}, 400

                # if everything goes well
                access_token = create_access_token(identity=current_user)  # email
                refresh_token = create_refresh_token(identity=current_user)

                return {'access_token': access_token, 'refresh_token': refresh_token}

        # ----

        # add login endpoint to flask/restplus api object
        # flask_api.add_resource(user_registration, '/registration')
        flask_api.add_resource(UserLogin, '/login')
        flask_api.add_resource(UserLogoutAccess, '/logout/access')
        flask_api.add_resource(UserLogoutRefresh, '/logout/refresh')
        flask_api.add_resource(TokenRefresh, '/token/refresh')

        # hack to fix error handling with flask restplus
        self.fix_jwt_errors(flask_api)

    # ----

    def fix_jwt_errors(self, flask_api=None):

        # flask_restplus and jwt-extented don't play well together on error messages
        # see: https://github.com/vimalloc/flask_jwt_extended/issues/86

        from jwt import ExpiredSignatureError, InvalidTokenError
        from flask_jwt_extended.exceptions import NoAuthorizationError, InvalidHeaderError, CSRFError, JWTDecodeError, \
            WrongTokenError, RevokedTokenError, FreshTokenRequired, UserLoadError, UserClaimsVerificationError

        @flask_api.errorhandler(NoAuthorizationError)
        def handle_auth_error(e):
            return {'message': str(e)}, 401

        @flask_api.errorhandler(CSRFError)
        def handle_auth_error(e):
            return {'message': str(e)}, 401

        @flask_api.errorhandler(ExpiredSignatureError)
        def handle_expired_error(e):
            return {'message': 'token has expired'}, 401

        @flask_api.errorhandler(InvalidHeaderError)
        def handle_invalid_header_error(e):
            return {'message': str(e)}, 422

        @flask_api.errorhandler(InvalidTokenError)
        def handle_invalid_token_error(e):
            return {'message': str(e)}, 422

        @flask_api.errorhandler(JWTDecodeError)
        def handle_jwt_decode_error(e):
            return {'message': str(e)}, 422

        @flask_api.errorhandler(WrongTokenError)
        def handle_wrong_token_error(e):
            return {'message': str(e)}, 422

        @flask_api.errorhandler(RevokedTokenError)
        def handle_revoked_token_error(e):
            return {'message': 'token has been revoked'}, 401

        @flask_api.errorhandler(FreshTokenRequired)
        def handle_fresh_token_required(e):
            return {'message': 'fresh token required'}, 401

        @flask_api.errorhandler(UserLoadError)
        def handler_user_load_error(e):
            # the identity is already saved before this Exception was raised,
            # otherwise a different Exception would be raised, which is why we
            # can safely call get_jwt_identity() here
            identity = get_jwt_identity()
            return {'message': "error loading the user {}".format(identity)}, 401

        @flask_api.errorhandler(UserClaimsVerificationError)
        def handle_failed_user_claims_verification(e):
            return {'message': 'user claims verification failed'}, 400

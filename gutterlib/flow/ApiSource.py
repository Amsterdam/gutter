"""
    ApiSource.py - sources an Rest API for data 
    
"""

import requests
import logging
import re
import simplejson as json

import string
import random

# Some Python2 backwards compatibility already
from future.standard_library import install_aliases

install_aliases()
from urllib.parse import urlparse, parse_qsl


class ApiSource:

    # ==== SETTINGS ====

    def get_placeholders_with_values(self, batch_num):
        return {'BATCH_NUM': batch_num, 'BATCH_NUM_PLUS_ONE': batch_num + 1}

    # ==== END SETTINGS ===

    def __init__(self, source_obj):

        """ Initiate a ApiSource
        
        :param source_obj: Dict with { get_token: "<<ID>>: example: sia", 
                url, token, rows_root:  root of row data in response json }
                
        """

        if isinstance(source_obj, dict):
            self.base_url = source_obj.get('url')  # URL can contain template placholders {{BATCH_NUM_PLUS_ONE}}
            self.cur_url = None  # filled URL
            self.get_special_token = source_obj.get(
                'get_special_token')  # special token function defined in this class: TODO howto seperate this?
            self.token_user = source_obj.get('token_user')
            self.token_password = source_obj.get('token_password')
            self.token = None  # the API token
            self.rows_root = source_obj.get('rows_root')  # for now only first level key. ie. results => data['results']

            # TODO for specific special login before API call
            self.inlog_curl = source_obj.get('inlog_curl')
            self.inlog_curl_token_re = source_obj.get('inlog_curl_token_re')

        self.url_variables = {'BATCH_NUM': 1}  # TODO: add batch_num, batch_start, batch_end, etc.

        self.setup_logger()

    # ----

    def setup_logger(self):

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

    def get_token(self):

        self.logger.info("Get token for API '{0}'".format(self.cur_url))

        if self.token is not None:
            # just return plane token
            return self.token

        # we have special function to get token
        if self.get_special_token is not None:

            if self.get_special_token == "SIA":
                # set token direct in instance
                self.token = self.get_sia_token(self.token_user, self.token_password)

                return self.token

    # ----

    def get_rows(self, url=None):

        """ Get rows from API call
        
        return: list of dict --
        
        """

        self.get_token()  # set direct in this instance for reference

        if url is None:
            self.fill_url_variables()
            url = self.cur_url

        if url is None:
            self.logger.error("Need URL to do a request!")
            return []

        headers = {}
        if self.token:
            headers = {"Authorization": "Bearer {0}".format(self.token)}

        try:
            data = requests.get(url, data={}, headers=headers).json()
        except Exception as e:
            self.logger.error("Cannot get data from API")
            return []

        if self.rows_root is not None:
            rows = data.get(self.rows_root)
        else:
            rows = data

        if data is None or rows is None:
            self.logger.error("get_rows: No rows found for url: '{0}'".format(url))
            return []

        return rows

    # ----

    def fill_url_variables(self, url=None, variables=None):

        LEFT_ANCHORS = '{{'
        RIGHT_ANCHORS = '}}'

        if url is None:
            url = self.base_url

        if variables is None:
            variables = self.url_variables

        place_holders = re.findall(LEFT_ANCHORS + '([^}]+)' + RIGHT_ANCHORS, url)

        for name in place_holders:
            value = variables.get(name)

            if value is not None:
                url = url.replace(LEFT_ANCHORS + name + RIGHT_ANCHORS, str(value))

        self.cur_url = url

        return url

    # ----

    def get_schema_definition(self, title=None):

        """ Simply get first first row, then make into json schema definition
        
        TODO: maybe check out more rows to figure out more data types
        
        
        """

        rows = self.get_batch_rows(0)

        if not isinstance(rows, list):
            self.logger.error("No test data to make schema definition!")
            return {}

        if len(rows) == 0:
            self.logger.error(
                "Could not get any rows to deduce schema from! check response of API for token '{0}'".format(
                    self.token))
            return {}

        first_row = rows[0]

        return {
            'title': title or self.base_url,
            'properties': self.get_schema_properties_for_dict(first_row)
        }

    # ----

    def get_schema_properties_for_dict(self, d):

        MAP_PYTHON_OBJECTS_TO_JSON_SCHEMA_OBJS = {
            'unicode': 'string',
            'str': 'string',
            'dict': 'object',
            'int': 'integer',
            'float': 'number',
            'long': 'number',
            'NoneType': 'string',
            'bool': 'boolean',
            'list': 'array'}

        FORMAT_TESTS_RE = {
            '[\d]{4}-[\d]{2}-[\d]{2}T| [\d]{2}\:[\d]{2}\:[\d]{2}\.[\d]{6}Z': 'date-time',
            '[\d]{4}-[\d]{2}-[\d]{2}T| [\d]{2}\:[\d]{2}\:[\d]{2}': 'date-time'
        }

        properties = {}

        for key, value in d.items():

            python_value_type = re.findall("\'([^\']+)\'", str(type(value)))[0]

            json_type = MAP_PYTHON_OBJECTS_TO_JSON_SCHEMA_OBJS.get(python_value_type)

            if json_type == 'object':
                nested_props = self.get_schema_properties_for_dict(value)

                properties[key] = {'type': json_type, 'properties': nested_props}

            else:
                properties[key] = {'type': json_type}

            # format
            for t in FORMAT_TESTS_RE:
                if re.match(t, str(value)):
                    properties[key]['format'] = FORMAT_TESTS_RE[t]
                    break

        return properties

    # ----

    def get_batch_rows(self, num):

        url_variables = self.get_placeholders_with_values(num)
        url = self.fill_url_variables(self.base_url, url_variables)

        rows = self.get_rows(url)

        return rows

    # ----

    def get_sia_token(self, email, password):

        """ Get a SIA token with some random magic ( just pasted from SIA repo ) 
        
        """

        def randomword(length):
            letters = string.ascii_lowercase
            return ''.join(random.choice(letters) for i in range(length))

        # NOTE: all these things are important: otherwise we get a 405 error
        state = randomword(10)
        scopes = ['SIG/ALL']
        authz_url = 'https://api.data.amsterdam.nl/oauth2/authorize'
        params = {
            'idp_id': 'datapunt',
            'response_type': 'token',
            'client_id': 'citydata',
            'scope': ' '.join(scopes),
            'state': state,
            'redirect_uri': 'https://data.amsterdam.nl/'
        }

        response = requests.get(authz_url, params, allow_redirects=False)
        if response.status_code == 303:
            location = response.headers["location"]
        else:
            self.logger.info("Cannot get SIA code!")
            return {}

        data = {
            'type': 'employee_plus',
            'email': email,
            'password': password,
        }

        response = requests.post(location, data=data, allow_redirects=False)

        if response.status_code == 303:
            location = response.headers["location"]
        else:
            self.logger.info("Cannot get SIA code in login phase! Check SIA credentials")
            return {}

        response = requests.get(location, allow_redirects=False)
        if response.status_code == 303:
            returned_url = response.headers["location"]
        else:
            self.logger.info("Cannot get SIA code in last get phase!")
            return {}

        # get grant_token from parameter aselect_credentials in session url
        parsed = urlparse(returned_url)
        fragment = parse_qsl(parsed.fragment)
        access_token = fragment[0][1]
        # os.environ["access_token"] = access_token

        if access_token is None:
            self.logger.error("No sia access token! Please check given 'token_user' and 'token_password' for SIA")
            return {}

        self.logger.info("Got SIA token: '{0}'".format(access_token))

        return access_token

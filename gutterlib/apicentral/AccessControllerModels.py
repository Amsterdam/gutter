"""

    gutterlib.apicentral.AccessControllerModels

"""

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, Boolean
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from passlib.hash import pbkdf2_sha256 as sha256

import uuid
import json

DBObj = declarative_base()


# ==== models =====

class ApiUser(DBObj):
    
    __tablename__ = 'api_users'
    __table_args__ = {"schema": "gutter"}

    id = Column(Integer(), primary_key=True)
    email = Column(String()) # functions as name
    admin = Column(Boolean()) # True or False
    password = Column(Text())  # hash
    allowed_apps = Column(ARRAY(Text())) # TODO
    rights_by_app = Column(JSONB()) # TODO

    # ----

    def __init__(self, email=None, password=None, admin=None, allowed_apps=None, rights_by_app=None):

        if email is None and password is None:
            # note: can be without parameters to only create table
            pass
        else:
            self.email = email
            self.admin = admin
            self.password = self.generate_hash(password)
            self.allowed_apps = allowed_apps
            self.rights_by_app = rights_by_app

    # ----

    def __repr__(self):
        # string/unicode representation of object
        return "<ApiUser id='{0}', email='{1}', admin='{2}', allowed_apps='{3}', rights_by_app='{4}>".format(
            self.id,
            self.email,
            self.admin,
            self.allowed_apps,
            self.rights_by_app)

    # ----

    def to_dict(self):

        # TODO: make this more robust?
        ALLOWED_PROPERTIES_OUTPUT = [unicode, str, bool, str, int, float, long, dict, list]

        # simple filter out internal sql alchemy keys
        d = {}

        for key in self.__dict__.keys():

            v = self.__dict__[key]

            if type(v) in ALLOWED_PROPERTIES_OUTPUT:  # key[0] != '_': # first char not '_'
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
            self.logger.error('create_table: Please supply engine')
            return False

        try:
            DBObj.metadata.create_all(engine)

        except Exception as e:
            print("ERROR: Can't create table for ApiUser: {0}".format(unicode(e)))

    # ----

    def generate_hash(self, password):

        return sha256.hash(password)

    # ----

    def verify_password(self, given_password):

        return sha256.verify(given_password, self.password)


# ====


class ApiRevokedToken(DBObj):
    __tablename__ = 'api_revoked_tokens'
    __table_args__ = {"schema": "gutter"}

    id = Column(Integer(), primary_key=True)
    jti = Column(String(120))

    # ----

    def __init__(self, jti=None):

        if jti is None:
            # note: can be without parameters to only create table
            pass
        else:
            self.jti = jti

    # ----

    def __repr__(self):

        # string/unicode representation of object

        return "<ApiRevokedToken id='{0}', jti='{1}'>".format(self.id, self.jti)

    # ----

    def to_dict(self):

        # TODO: make this more robust?
        allowed_properties_output = [unicode, str, bool, str, int, float, long, dict, list]

        # simple filter out internal sql alchemy keys
        d = {}

        for key in self.__dict__.keys():

            v = self.__dict__[key]

            if type(v) in allowed_properties_output:  # key[0] != '_': # first char not '_'
                if (type(v) is unicode or type(v) is str) and v is not None:
                    d[key] = v.encode('utf8', errors='ignore')  # force utf_8 encoding
                else:
                    d[key] = v

        return d

    # ----

    def create_table(self, engine):

        if not engine:
            self.logger.error('create_table: Please supply engine')
            return False

        try:
            DBObj.metadata.create_all(engine)

        except Exception as e:
            print("ERROR: Can't create table for ApiRevokedToken: {0}".format(unicode(e)))

# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                    |
# |                                                                          |
# | (C) Copyright IBM Corporation 2009-2018.                                      |
# +--------------------------------------------------------------------------+
# | This module complies with Django 1.0 and is                              |
# | Licensed under the Apache License, Version 2.0 (the "License");          |
# | you may not use this file except in compliance with the License.         |
# | You may obtain a copy of the License at                                  |
# | http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable |
# | law or agreed to in writing, software distributed under the License is   |
# | distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY |
# | KIND, either express or implied. See the License for the specific        |
# | language governing permissions and limitations under the License.        |
# +--------------------------------------------------------------------------+
# | Authors: Ambrish Bhargava, Tarun Pasrija, Rahul Priyadarshi              |
# +--------------------------------------------------------------------------+

"""
DB2 database backend for Django.
Requires: pyodbc for python
"""
import sys

from django.core.exceptions import ImproperlyConfigured

# Importing class from base module of django.db.backends

try:
    from django.db.backends import BaseDatabaseFeatures
except ImportError:
    from django.db.backends.base.features import BaseDatabaseFeatures

try:
    from django.db.backends import BaseDatabaseWrapper
except ImportError:
    from django.db.backends.base.base import BaseDatabaseWrapper

try:
    from django.db.backends import BaseDatabaseValidation
except ImportError:
    from django.db.backends.base.validation import BaseDatabaseValidation

from django.db.backends.signals import connection_created

# Importing internal classes from django_ibmi package.
from django_ibmi.client import DatabaseClient
from django_ibmi.creation import DatabaseCreation
from django_ibmi.introspection import DatabaseIntrospection
from django_ibmi.operations import DatabaseOperations

import django_ibmi.pybase as Base
import pyodbc

    
# For checking django's version
from django import VERSION as djangoVersion

if ( djangoVersion[0:2] >= ( 1, 7 )):
    from django_ibmi.schemaEditor import DB2SchemaEditor

DatabaseError = pyodbc.DatabaseError
IntegrityError = pyodbc.IntegrityError
if ( djangoVersion[0:2] >= ( 1, 6 )):
    Error = pyodbc.Error
    InterfaceError = pyodbc.InterfaceError
    DataError = pyodbc.DataError
    OperationalError = pyodbc.OperationalError
    InternalError = pyodbc.InternalError
    ProgrammingError = pyodbc.ProgrammingError
    NotSupportedError = pyodbc.NotSupportedError
    

dbms_name = 'dbms_name'

    
class DatabaseFeatures( BaseDatabaseFeatures ):    
    can_use_chunked_reads = True
    
    #Save point is supported by DB2.
    uses_savepoints = True
    
    #Custom query class has been implemented 
    #django.db.backends.db2.query.query_class.DB2QueryClass
    uses_custom_query_class = True
    
    #transaction is supported by DB2
    supports_transactions = True
    
    supports_tablespaces = True
    
    uppercases_column_names = True
    interprets_empty_strings_as_nulls = False
    allows_primary_key_0 = True
    can_defer_constraint_checks = False
    supports_forward_references = False
    requires_rollback_on_dirty_transaction = True
    supports_regex_backreferencing = True
    supports_timezones = False
    has_bulk_insert = False
    has_select_for_update = True
    supports_long_model_names = False
    can_distinct_on_fields = False
    supports_paramstyle_pyformat = False
    supports_sequence_reset = True
    #DB2 doesn't take default values as parameter
    requires_literal_defaults = True
    has_case_insensitive_like = True
    can_introspect_big_integer_field = True
    can_introspect_boolean_field = False
    can_introspect_positive_integer_field = False
    can_introspect_small_integer_field = True
    can_introspect_null = True
    can_introspect_ip_address_field = False
    can_introspect_time_field = True
    
class DatabaseValidation( BaseDatabaseValidation ):    
    #Need to do validation for IBM i and pyodbc version
    def validate_field( self, errors, opts, f ):
        pass

class DatabaseWrapper( BaseDatabaseWrapper ):
    
    """
    This is the base class for DB2 backend support for Django. The under lying 
    wrapper is pyodbc.
    """
    data_types={}
    vendor = 'DB2'
    operators = {
        "exact":        "= %s",
        "iexact":       "LIKE UPPER(%s) ESCAPE '\\'",
        "contains":     "LIKE %s ESCAPE '\\'",
        "icontains":    "LIKE UPPER(%s) ESCAPE '\\'",
        "gt":           "> %s",
        "gte":          ">= %s",
        "lt":           "< %s",
        "lte":          "<= %s",
        "startswith":   "LIKE %s ESCAPE '\\'",
        "endswith":     "LIKE %s ESCAPE '\\'",
        "istartswith":  "LIKE UPPER(%s) ESCAPE '\\'",
        "iendswith":    "LIKE UPPER(%s) ESCAPE '\\'",
    }
    if( djangoVersion[0:2] >= ( 1, 6 ) ):
        Database = pyodbc

    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    validation_class = DatabaseValidation
    ops_class = DatabaseOperations
 
    # Constructor of DB2 backend support. Initializing all other classes.
    def __init__( self, *args ):
        super( DatabaseWrapper, self ).__init__( *args )
        self.ops = DatabaseOperations( self )
        if( djangoVersion[0:2] <= ( 1, 0 ) ):
            self.client = DatabaseClient()
        else:
            self.client = DatabaseClient( self )
        if( djangoVersion[0:2] <= ( 1, 2 ) ):
            self.features = DatabaseFeatures()
        else:
            self.features = DatabaseFeatures( self )
        self.creation = DatabaseCreation( self )
        
        if( djangoVersion[0:2] >= ( 1, 8 ) ): 
            self.data_types=self.creation.data_types
            self.data_type_check_constraints=self.creation.data_type_check_constraints
        
        self.introspection = DatabaseIntrospection( self )
        if( djangoVersion[0:2] <= ( 1, 1 ) ):
            self.validation = DatabaseValidation()
        else:
            self.validation = DatabaseValidation( self )
        self.databaseWrapper = Base.DatabaseWrapper()
    
    # Method to check if connection is live or not.
    def __is_connection( self ):
        return self.connection is not None
    
    # To get dict of connection parameters 
    def get_connection_params(self):
        if sys.version_info.major >= 3:
            strvar = str
        else:
            strvar = basestring
        kwargs = { }
        if ( djangoVersion[0:2] <= ( 1, 0 ) ):
            database_name = self.settings.DATABASE_NAME
            database_user = self.settings.DATABASE_USER
            database_pass = self.settings.DATABASE_PASSWORD
            database_host = self.settings.DATABASE_HOST
            database_port = self.settings.DATABASE_PORT
            database_options = self.settings.DATABASE_OPTIONS
        elif ( djangoVersion[0:2] <= ( 1, 1 ) ):
            settings_dict = self.settings_dict
            database_name = settings_dict['DATABASE_NAME']
            database_user = settings_dict['DATABASE_USER']
            database_pass = settings_dict['DATABASE_PASSWORD']
            database_host = settings_dict['DATABASE_HOST']
            database_port = settings_dict['DATABASE_PORT']
            database_options = settings_dict['DATABASE_OPTIONS']
        else:
            settings_dict = self.settings_dict
            database_name = settings_dict['NAME']
            database_user = settings_dict['USER']
            database_pass = settings_dict['PASSWORD']
            database_host = settings_dict['HOST']
            database_port = settings_dict['PORT']
            database_options = settings_dict['OPTIONS']
 
        if database_name != '' and isinstance( database_name, strvar ):
            kwargs['database'] = database_name
        else:
            raise ImproperlyConfigured( "Please specify the valid database Name to connect to" )
            
        if isinstance( database_user, strvar ):
            kwargs['user'] = database_user
        
        if isinstance( database_pass, strvar ):
            kwargs['password'] = database_pass
        
        if isinstance( database_host, strvar ):
            kwargs['host'] = database_host
        
        if isinstance( database_port, strvar ):
            kwargs['port'] = database_port
            
        if isinstance( database_host, strvar ):
            kwargs['host'] = database_host
        
        if isinstance( database_options, dict ):
            kwargs['options'] = database_options
        
        if ( djangoVersion[0:2] <= ( 1, 0 ) ):
           if( hasattr( settings, 'PCONNECT' ) ):
               kwargs['PCONNECT'] = settings.PCONNECT
        else:
            if ( settings_dict.keys() ).__contains__( 'PCONNECT' ):
                kwargs['PCONNECT'] = settings_dict['PCONNECT']

        if('CURRENTSCHEMA' in settings_dict):
            database_schema = settings_dict['CURRENTSCHEMA']
            if isinstance( database_schema, str ):
                kwargs['currentschema'] = database_schema

        if('SECURITY'  in settings_dict):
            database_security = settings_dict['SECURITY']
            if isinstance( database_security, str ):
                kwargs['security'] = database_security

        if('SSLCLIENTKEYDB'  in settings_dict):
            database_sslclientkeydb = settings_dict['SSLCLIENTKEYDB']
            if isinstance( database_sslclientkeydb, str ):
                kwargs['sslclientkeydb'] = database_sslclientkeydb

        if('SSLCLIENTKEYSTOREDBPASSWORD'  in settings_dict):
            database_sslclientkeystoredbpassword = settings_dict['SSLCLIENTKEYSTOREDBPASSWORD']
            if isinstance( database_sslclientkeystoredbpassword, str ):
                kwargs['sslclientkeystoredbpassword'] = database_sslclientkeystoredbpassword

        if('SSLCLIENTKEYSTASH'  in settings_dict):
            database_sslclientkeystash =settings_dict['SSLCLIENTKEYSTASH']
            if isinstance( database_sslclientkeystash, str ):
                kwargs['sslclientkeystash'] = database_sslclientkeystash

        if('SSLSERVERCERTIFICATE'  in settings_dict):
            database_sslservercertificate =settings_dict['SSLSERVERCERTIFICATE']
            if isinstance( database_sslservercertificate, str ):
                kwargs['sslservercertificate'] = database_sslservercertificate

        return kwargs
    
    # To get new connection from Database
    def get_new_connection(self, conn_params):
        connection = self.databaseWrapper.get_new_connection(conn_params)
        self.features.has_bulk_insert = True
        return connection
        
    # Over-riding _cursor method to return DB2 cursor.
    if ( djangoVersion[0:2] < ( 1, 6 )):
        def _cursor( self, settings = None ):
            if not self.__is_connection():
                if ( djangoVersion[0:2] <= ( 1, 0 ) ):
                    self.settings = settings
                    
                self.connection = self.get_new_connection(self.get_connection_params())
                cursor = self.databaseWrapper._cursor(self.connection)
                
                if( djangoVersion[0:3] <= ( 1, 2, 2 ) ):
                    connection_created.send( sender = self.__class__ )
                else:
                    connection_created.send( sender = self.__class__, connection = self )
            else:
                cursor = self.databaseWrapper._cursor( self.connection )  
            return cursor
    else:
        def create_cursor( self , name = None):
            return self.databaseWrapper._cursor( self.connection )
            
        def init_connection_state( self ):
            pass
        
        def is_usable(self):
            if self.databaseWrapper.is_active( self.connection ):
                return True
            else:
                return False
            
    def _set_autocommit(self, autocommit):
        self.connection.autocommit = autocommit
     
    def close( self ):
        if( djangoVersion[0:2] >= ( 1, 5 ) ):
            self.validate_thread_sharing()
        if self.connection is not None:
            self.databaseWrapper.close( self.connection )
            self.connection = None
        
    def get_server_version( self ):
        if not self.connection:
            self.cursor()
        return self.databaseWrapper.get_server_version( self.connection )
    
    def schema_editor(self, *args, **kwargs):
        return DB2SchemaEditor(self, *args, **kwargs)
   
    
    
            

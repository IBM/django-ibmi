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
Requires: ibm_db_dbi (http://pypi.python.org/pypi/ibm_db) for python
"""
import sys
import datetime

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




# Importing internal classes from ibm_db_django package.
from ibm_db_django.client import DatabaseClient
from ibm_db_django.creation import DatabaseCreation
from ibm_db_django.introspection import DatabaseIntrospection
from ibm_db_django.operations import DatabaseOperations
import pyodbc as Database

# For checking django's version
from django import VERSION as djangoVersion

if (djangoVersion[0:2] > (1, 1)):
    from django.db import utils
    import sys
if (djangoVersion[0:2] >= (1, 4)):
    from django.utils import timezone
    from django.conf import settings
    import warnings
if (djangoVersion[0:2] >= (1, 5)):
    from django.utils.encoding import force_bytes, force_text
    from django.utils import six
    import re

if ( djangoVersion[0:2] >= ( 1, 7 )):
    from ibm_db_django.schemaEditor import DB2SchemaEditor

dbms_name = 'dbms_name'

DatabaseError = Database.DatabaseError
IntegrityError = Database.IntegrityError
if ( djangoVersion[0:2] >= ( 1, 6 )):
    Error = Database.Error
    InterfaceError = Database.InterfaceError
    DataError = Database.DataError
    OperationalError = Database.OperationalError
    InternalError = Database.InternalError
    ProgrammingError = Database.ProgrammingError
    NotSupportedError = Database.NotSupportedError
    
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
    #Need to do validation for DB2 and ibm_db version
    def validate_field( self, errors, opts, f ):
        pass

class DatabaseWrapper( BaseDatabaseWrapper ):
    
    """
    This is the base class for DB2 backend support for Django. The under lying 
    wrapper is IBM_DB_DBI (latest version can be downloaded from http://code.google.com/p/ibm-db/ or
    http://pypi.python.org/pypi/ibm_db). 
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
        Database = Database

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
        self.features.has_bulk_insert = True
        return Database.connect(conn_params)
        
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
            return DB2CursorWrapper(self.connection)

        def init_connection_state( self ):
            pass

        def is_usable(self):
            try:
                self.connection.ping()
            except Database.Error:
                return False
            else:
                return True
            
    def _set_autocommit(self, autocommit):
        self.connection.set_autocommit( autocommit )
     
    def close( self ):
        if( djangoVersion[0:2] >= ( 1, 5 ) ):
            self.validate_thread_sharing()
        if self.connection is not None:
            self.connection.close()
            self.connection = None
    
    def schema_editor(self, *args, **kwargs):
        return DB2SchemaEditor(self, *args, **kwargs)

    def get_server_version(self, connection):
        self.connection = connection
        if not self.connection:
            self.cursor()
        return tuple(int(version) for version in
                     self.connection.server_info()[1].split("."))


class DB2CursorWrapper(Database.Cursor):
    """
    This is the wrapper around IBM_DB_DBI in order to support format parameter style
    IBM_DB_DBI supports qmark, where as Django support format style,
    hence this conversion is required.
    """

    def __init__(self, connection):
        super(DB2CursorWrapper, self).__init__(connection.conn_handler,
                                               connection)

    def __iter__(self):
        return self

    def next(self):
        row = self.fetchone()
        if row == None:
            raise StopIteration
        return row

    def _create_instance(self, connection):
        return DB2CursorWrapper(connection)

    def _format_parameters(self, parameters):
        parameters = list(parameters)
        for index in range(len(parameters)):
            # With raw SQL queries, datetimes can reach this function
            # without being converted by DateTimeField.get_db_prep_value.
            if settings.USE_TZ and isinstance(parameters[index],
                                              datetime.datetime):
                param = parameters[index]
                if timezone.is_naive(param):
                    warnings.warn(u"Received a naive datetime (%s)"
                                  u" while time zone support is active." % param,
                                  RuntimeWarning)
                    default_timezone = timezone.get_default_timezone()
                    param = timezone.make_aware(param, default_timezone)
                param = param.astimezone(timezone.utc).replace(tzinfo=None)
                parameters[index] = param
        return tuple(parameters)

    # Over-riding this method to modify SQLs which contains format parameter to qmark.
    def execute(self, operation, parameters=()):
        if (djangoVersion[0:2] >= (2, 0)):
            operation = str(operation)
        try:
            if operation.find('ALTER TABLE') == 0 and getattr(self.connection,
                                                              dbms_name) != 'DB2':
                doReorg = 1
            else:
                doReorg = 0
            if operation.count("db2regexExtraField(%s)") > 0:
                operation = operation.replace("db2regexExtraField(%s)", "")
                operation = operation % parameters
                parameters = ()
            if operation.count("%s") > 0:
                operation = operation % (tuple("?" * operation.count("%s")))
            if (djangoVersion[0:2] >= (1, 4)):
                parameters = self._format_parameters(parameters)

            if (djangoVersion[0:2] <= (1, 1)):
                if (doReorg == 1):
                    super(DB2CursorWrapper, self).execute(operation, parameters)
                    return self._reorg_tables()
                else:
                    return super(DB2CursorWrapper, self).execute(operation,
                                                                 parameters)
            else:
                try:
                    if (doReorg == 1):
                        super(DB2CursorWrapper, self).execute(operation,
                                                              parameters)
                        return self._reorg_tables()
                    else:
                        return super(DB2CursorWrapper, self).execute(operation,
                                                                     parameters)
                except IntegrityError as e:
                    if (djangoVersion[0:2] >= (1, 5)):
                        six.reraise(utils.IntegrityError, utils.IntegrityError(
                            *tuple(six.PY3 and e.args or (e._message,))),
                                    sys.exc_info()[2])
                        raise
                    else:
                        raise utils.IntegrityError, utils.IntegrityError(
                            *tuple(e)), sys.exc_info()[2]

                except ProgrammingError as e:
                    if (djangoVersion[0:2] >= (1, 5)):
                        six.reraise(utils.ProgrammingError,
                                    utils.ProgrammingError(*tuple(
                                        six.PY3 and e.args or (e._message,))),
                                    sys.exc_info()[2])
                        raise
                    else:
                        raise utils.ProgrammingError, utils.ProgrammingError(
                            *tuple(e)), sys.exc_info()[2]
                except DatabaseError as e:
                    if (djangoVersion[0:2] >= (1, 5)):
                        six.reraise(utils.DatabaseError, utils.DatabaseError(
                            *tuple(six.PY3 and e.args or (e._message,))),
                                    sys.exc_info()[2])
                        raise
                    else:
                        raise utils.DatabaseError, utils.DatabaseError(
                            *tuple(e)), sys.exc_info()[2]
        except (TypeError):
            return None

    # Over-riding this method to modify SQLs which contains format parameter to qmark.
    def executemany(self, operation, seq_parameters):
        try:
            if operation.count("db2regexExtraField(%s)") > 0:
                raise ValueError("Regex not supported in this operation")
            if operation.count("%s") > 0:
                operation = operation % (tuple("?" * operation.count("%s")))
            if (djangoVersion[0:2] >= (1, 4)):
                seq_parameters = [self._format_parameters(parameters) for
                                  parameters in seq_parameters]

            if (djangoVersion[0:2] <= (1, 1)):
                return super(DB2CursorWrapper, self).executemany(operation,
                                                                 seq_parameters)
            else:
                try:
                    return super(DB2CursorWrapper, self).executemany(operation,
                                                                     seq_parameters)
                except IntegrityError as e:
                    if (djangoVersion[0:2] >= (1, 5)):
                        six.reraise(utils.IntegrityError, utils.IntegrityError(
                            *tuple(six.PY3 and e.args or (e._message,))),
                                    sys.exc_info()[2])
                        raise
                    else:
                        raise utils.IntegrityError, utils.IntegrityError(
                            *tuple(e)), sys.exc_info()[2]
                except DatabaseError as e:
                    if (djangoVersion[0:2] >= (1, 5)):
                        six.reraise(utils.DatabaseError, utils.DatabaseError(
                            *tuple(six.PY3 and e.args or (e._message,))),
                                    sys.exc_info()[2])
                        raise
                    else:
                        raise utils.DatabaseError, utils.DatabaseError(
                            *tuple(e)), sys.exc_info()[2]
        except (IndexError, TypeError):
            return None

    # table reorganization method
    def _reorg_tables(self):
        checkReorgSQL = "select TABSCHEMA, TABNAME from SYSIBMADM.ADMINTABINFO where REORG_PENDING = 'Y'"
        res = []
        reorgSQLs = []
        parameters = ()
        super(DB2CursorWrapper, self).execute(checkReorgSQL, parameters)
        res = super(DB2CursorWrapper, self).fetchall()
        if res:
            for sName, tName in res:
                reorgSQL = '''CALL SYSPROC.ADMIN_CMD('REORG TABLE "%(sName)s"."%(tName)s"')''' % {
                    'sName': sName, 'tName': tName}
                reorgSQLs.append(reorgSQL)
            for sql in reorgSQLs:
                super(DB2CursorWrapper, self).execute(sql)

    # Over-riding this method to modify result set containing datetime and time zone support is active
    def fetchone(self):
        row = super(DB2CursorWrapper, self).fetchone()
        if row is None:
            return row
        else:
            return self._fix_return_data(row)

    # Over-riding this method to modify result set containing datetime and time zone support is active
    def fetchmany(self, size=0):
        rows = super(DB2CursorWrapper, self).fetchmany(size)
        if rows is None:
            return rows
        else:
            return [self._fix_return_data(row) for row in rows]

    # Over-riding this method to modify result set containing datetime and time zone support is active
    def fetchall(self):
        rows = super(DB2CursorWrapper, self).fetchall()
        if rows is None:
            return rows
        else:
            return [self._fix_return_data(row) for row in rows]

    # This method to modify result set containing datetime and time zone support is active
    def _fix_return_data(self, row):
        row = list(row)
        index = -1
        if (djangoVersion[0:2] >= (1, 4)):
            for value, desc in zip(row, self.description):
                index = index + 1
                if (desc[1] == Database.DATETIME):
                    if settings.USE_TZ and value is not None and timezone.is_naive(
                            value):
                        value = value.replace(tzinfo=timezone.utc)
                        row[index] = value
                elif (djangoVersion[0:2] >= (1, 5)):
                    if isinstance(value, six.string_types):
                        row[index] = re.sub(r'[\x00]', '', value)
        return tuple(row)

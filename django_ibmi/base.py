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
Requires: pyodbc
"""

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


# Importing internal classes from django_ibmi package.
from .client import DatabaseClient
from .creation import DatabaseCreation
from .introspection import DatabaseIntrospection
from .operations import DatabaseOperations

import pyodbc

from .schemaEditor import DB2SchemaEditor

import datetime
from django.db import utils
from django.utils import timezone
from django.conf import settings
import warnings
import re

DatabaseError = pyodbc.DatabaseError
IntegrityError = pyodbc.IntegrityError
Error = pyodbc.Error
InterfaceError = pyodbc.InterfaceError
DataError = pyodbc.DataError
OperationalError = pyodbc.OperationalError
InternalError = pyodbc.InternalError
ProgrammingError = pyodbc.ProgrammingError
NotSupportedError = pyodbc.NotSupportedError


dbms_name = 'dbms_name'


class DatabaseFeatures(BaseDatabaseFeatures):
    can_use_chunked_reads = True

    # Save point is supported by DB2.
    uses_savepoints = True

    # Custom query class has been implemented
    # django.db.backends.db2.query.query_class.DB2QueryClass
    uses_custom_query_class = True

    # transaction is supported by DB2
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
    # DB2 doesn't take default values as parameter
    requires_literal_defaults = True
    has_case_insensitive_like = True
    can_introspect_big_integer_field = True
    can_introspect_boolean_field = False
    can_introspect_positive_integer_field = False
    can_introspect_small_integer_field = True
    can_introspect_null = True
    can_introspect_ip_address_field = False
    can_introspect_time_field = True


class DatabaseValidation(BaseDatabaseValidation):
    # Need to do validation for IBM i and pyodbc version
    def validate_field(self, errors, opts, f):
        pass


class DatabaseWrapper(BaseDatabaseWrapper):

    """
    This is the base class for DB2 backend support for Django. The under lying
    wrapper is pyodbc.
    """
    data_types = {}
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

    Database = pyodbc

    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    validation_class = DatabaseValidation
    ops_class = DatabaseOperations

    # Constructor of DB2 backend support. Initializing all other classes.
    def __init__(self, *args):
        super().__init__(*args)
        self.ops = DatabaseOperations(self)
        self.client = DatabaseClient(self)
        self.features = DatabaseFeatures(self)
        self.creation = DatabaseCreation(self)
        self.data_types = self.creation.data_types
        self.data_type_check_constraints = self.creation.data_type_check_constraints
        self.introspection = DatabaseIntrospection(self)
        self.validation = DatabaseValidation(self)
        self.databaseWrapper = DatabaseWrapper()

    # Method to check if connection is live or not.
    def __is_connection(self):
        return self.connection is not None

    # To get dict of connection parameters
    def get_connection_params(self):
        kwargs = {}

        settings_dict = self.settings_dict
        database_name = settings_dict['NAME']
        database_user = settings_dict['USER']
        database_pass = settings_dict['PASSWORD']
        database_host = settings_dict['HOST']
        database_port = settings_dict['PORT']
        database_options = settings_dict['OPTIONS']

        if database_name != '' and isinstance(database_name, str):
            kwargs['database'] = database_name
        else:
            raise ImproperlyConfigured(
                "Please specify the valid database Name to connect to")

        if isinstance(database_user, str):
            kwargs['user'] = database_user

        if isinstance(database_pass, str):
            kwargs['password'] = database_pass

        if isinstance(database_host, str):
            kwargs['host'] = database_host

        if isinstance(database_port, str):
            kwargs['port'] = database_port

        if isinstance(database_host, str):
            kwargs['host'] = database_host

        if isinstance(database_options, dict):
            kwargs['options'] = database_options

        if (settings_dict.keys()).__contains__('PCONNECT'):
            kwargs['PCONNECT'] = settings_dict['PCONNECT']

        if 'CURRENTSCHEMA' in settings_dict:
            database_schema = settings_dict['CURRENTSCHEMA']
            if isinstance(database_schema, str):
                kwargs['currentschema'] = database_schema

        if 'SECURITY' in settings_dict:
            database_security = settings_dict['SECURITY']
            if isinstance(database_security, str):
                kwargs['security'] = database_security

        if 'SSLCLIENTKEYDB' in settings_dict:
            database_sslclientkeydb = settings_dict['SSLCLIENTKEYDB']
            if isinstance(database_sslclientkeydb, str):
                kwargs['sslclientkeydb'] = database_sslclientkeydb

        if 'SSLCLIENTKEYSTOREDBPASSWORD' in settings_dict:
            database_sslclientkeystoredbpassword = settings_dict['SSLCLIENTKEYSTOREDBPASSWORD']
            if isinstance(database_sslclientkeystoredbpassword, str):
                kwargs['sslclientkeystoredbpassword'] = database_sslclientkeystoredbpassword

        if 'SSLCLIENTKEYSTASH' in settings_dict:
            database_sslclientkeystash = settings_dict['SSLCLIENTKEYSTASH']
            if isinstance(database_sslclientkeystash, str):
                kwargs['sslclientkeystash'] = database_sslclientkeystash

        if 'SSLSERVERCERTIFICATE' in settings_dict:
            database_sslservercertificate = settings_dict['SSLSERVERCERTIFICATE']
            if isinstance(database_sslservercertificate, str):
                kwargs['sslservercertificate'] = database_sslservercertificate

        return kwargs

    # To get new connection from Database
    def get_new_connection(self, conn_params):
        SchemaFlag = False
        kwargs = conn_params
        kwargsKeys = kwargs.keys()
        if (kwargsKeys.__contains__('port') and
                kwargsKeys.__contains__('host')):
            kwargs[
                'dsn'] = "DATABASE=%s;HOSTNAME=%s;PORT=%s;PROTOCOL=TCPIP;" % (
                kwargs.get('database'),
                kwargs.get('host'),
                kwargs.get('port')
            )
        else:
            kwargs['dsn'] = kwargs.get('database')

        if kwargsKeys.__contains__('currentschema'):
            kwargs['dsn'] += "CurrentSchema=%s;" % (
                kwargs.get('currentschema'))
            SchemaFlag = True
            del kwargs['currentschema']

        if kwargsKeys.__contains__('security'):
            kwargs['dsn'] += "security=%s;" % (kwargs.get('security'))
            del kwargs['security']

        if kwargsKeys.__contains__('sslclientkeystoredb'):
            kwargs['dsn'] += "SSLCLIENTKEYSTOREDB=%s;" % (
                kwargs.get('sslclientkeystoredb'))
            del kwargs['sslclientkeystoredb']

        if kwargsKeys.__contains__('sslclientkeystoredbpassword'):
            kwargs['dsn'] += "SSLCLIENTKEYSTOREDBPASSWORD=%s;" % (
                kwargs.get('sslclientkeystoredbpassword'))
            del kwargs['sslclientkeystoredbpassword']

        if kwargsKeys.__contains__('sslclientkeystash'):
            kwargs['dsn'] += "SSLCLIENTKEYSTASH=%s;" % (
                kwargs.get('sslclientkeystash'))
            del kwargs['sslclientkeystash']

        if kwargsKeys.__contains__('sslservercertificate'):
            kwargs['dsn'] += "SSLSERVERCERTIFICATE=%s;" % (
                kwargs.get('sslservercertificate'))
            del kwargs['sslservercertificate']

        if kwargsKeys.__contains__('options'):
            kwargs.update(kwargs.get('options'))
            del kwargs['options']
        if kwargsKeys.__contains__('port'):
            del kwargs['port']

        if kwargsKeys.__contains__('PCONNECT'):
            del kwargs['PCONNECT']

        connection = pyodbc.connect(**kwargs)
        connection.autocommit = connection.set_autocommit

        if SchemaFlag:
            # TODO implement set_current_schema
            # schema = connection.set_current_schema(currentschema)
            pass
        self.features.has_bulk_insert = True
        return connection

    def create_cursor(self, name=None):
        return DB2CursorWrapper(self.connection)

    def init_connection_state(self):
        pass

    def is_usable(self):
        # TODO implement is_usable method correctly
        return True

    def _set_autocommit(self, autocommit):
        self.connection.autocommit = autocommit

    def close(self):
        self.validate_thread_sharing()
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def get_server_version(self):
        if not self.connection:
            self.cursor()
        return tuple(int(version) for version in
                     self.connection.server_info()[1].split("."))

    def schema_editor(self, *args, **kwargs):
        return DB2SchemaEditor(self, *args, **kwargs)


class DB2CursorWrapper(pyodbc.Cursor):
    """
    This is the wrapper around pyodbc in order to support format parameter style
    pyodbc supports qmark, where as Django support format style,
    hence this conversion is required.
    """

    def __init__(self, connection):
        super().__init__(connection.conn_handler, connection)

    def __iter__(self):
        return self

    def next(self):
        row = self.fetchone()
        if row is None:
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
                    warnings.warn("Received a naive datetime (%s)"
                                  " while time zone support is active." % param,
                                  RuntimeWarning)
                    default_timezone = timezone.get_default_timezone()
                    param = timezone.make_aware(param, default_timezone)
                param = param.astimezone(timezone.utc).replace(tzinfo=None)
                parameters[index] = param
        return tuple(parameters)

    # Over-riding this method to modify SQLs which contains format parameter
    # to qmark.
    def execute(self, operation, parameters=()):
        operation = str(operation)
        try:

            doReorg = 1 if operation.find('ALTER TABLE') == 0 and getattr(self.connection, dbms_name) != 'DB2' else 0
            if operation.count("db2regexExtraField(%s)") > 0:
                operation = operation.replace("db2regexExtraField(%s)", "")
                operation = operation % parameters
                parameters = ()
            if operation.count("%s") > 0:
                operation = operation % (tuple("?" * operation.count("%s")))
            parameters = self._format_parameters(parameters)

            try:
                super().execute(operation, parameters)
                if doReorg == 1:
                    return self._reorg_tables()
            except IntegrityError as e:
                raise utils.IntegrityError(*e.args) from e

            except ProgrammingError as e:
                raise utils.ProgrammingError(*e.args) from e

            except DatabaseError as e:
                raise utils.DatabaseError(*e.args) from e

        except TypeError:
            return None

    # Over-riding this method to modify SQLs which contains format parameter to qmark.
    def executemany(self, operation, seq_parameters):
        try:
            if operation.count("db2regexExtraField(%s)") > 0:
                raise ValueError("Regex not supported in this operation")
            if operation.count("%s") > 0:
                operation = operation % (tuple("?" * operation.count("%s")))

            seq_parameters = [self._format_parameters(parameters) for
                              parameters in seq_parameters]
            try:
                return super().executemany(operation, seq_parameters)
            except IntegrityError as e:
                raise utils.IntegrityError(*e.args) from e

            except DatabaseError as e:
                raise utils.DatabaseError(*e.args) from e

        except (IndexError, TypeError):
            return None

    # table reorganization method
    def _reorg_tables(self):
        checkReorgSQL = "select TABSCHEMA, TABNAME from SYSIBMADM.ADMINTABINFO where REORG_PENDING = 'Y'"
        res = []
        reorgSQLs = []
        parameters = ()
        super().execute(checkReorgSQL, parameters)
        res = super().fetchall()
        if res:
            for sName, tName in res:
                reorgSQL = '''CALL SYSPROC.ADMIN_CMD('REORG TABLE "%(sName)s"."%(tName)s"')''' % {
                    'sName': sName, 'tName': tName}
                reorgSQLs.append(reorgSQL)
            for sql in reorgSQLs:
                super().execute(sql)

    # Over-riding this method to modify result set containing datetime and time zone support is active
    def fetchone(self):
        row = super().fetchone()
        if row is None:
            return row
        else:
            return self._fix_return_data(row)

    # Over-riding this method to modify result set containing datetime and time zone support is active
    def fetchmany(self, size=0):
        rows = super().fetchmany(size)
        if rows is None:
            return rows
        else:
            return [self._fix_return_data(row) for row in rows]

    # Over-riding this method to modify result set containing datetime and time zone support is active
    def fetchall(self):
        rows = super().fetchall()
        if rows is None:
            return rows
        else:
            return [self._fix_return_data(row) for row in rows]

    # This method to modify result set containing datetime and time zone support is active
    def _fix_return_data(self, row):
        row = list(row)
        index = -1
        for value, desc in zip(row, self.description):
            index = index + 1
            if (desc[1] == pyodbc.DATETIME):
                if settings.USE_TZ and value is not None and timezone.is_naive(
                        value):
                    value = value.replace(tzinfo=timezone.utc)
                    row[index] = value
            if isinstance(value, str):
                row[index] = re.sub(r'[\x00]', '', value)
        return tuple(row)

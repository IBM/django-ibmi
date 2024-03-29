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
from distutils import util

from django.core.exceptions import ImproperlyConfigured

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
from .features import DatabaseFeatures

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
        settings_dict = self.settings_dict
        if 'NAME' not in settings_dict:
            raise ImproperlyConfigured(
                "settings.DATABASES is improperly configured. "
                "Please supply the NAME value.")
        conn_params = {
            'system': settings_dict['NAME']
        }
        if 'USER' in settings_dict:
            conn_params['user'] = settings_dict['USER']
        if 'PASSWORD' in settings_dict:
            conn_params['password'] = settings_dict['PASSWORD']

        if 'OPTIONS' in settings_dict:
            conn_params.update(settings_dict['OPTIONS'])

        allowed_opts = {'system', 'user', 'password', 'autocommit',
                        'readonly',
                        'timeout', 'database', 'use_system_naming',
                        'library_list', 'current_schema'
                        }

        if not allowed_opts.issuperset(conn_params.keys()):
            raise ValueError("Option entered not valid for "
                             "IBM i Access ODBC Driver")

        try:
            conn_params['Naming'] = \
                str(util.strtobool(conn_params['use_system_naming']))
        except (ValueError, KeyError):
            conn_params['Naming'] = '0'

        if 'current_schema' in conn_params or 'library_list' in conn_params:
            conn_params['DefaultLibraries'] = \
                conn_params.pop('current_schema', '') + ','
            library_list = conn_params.pop('library_list', '')
            if isinstance(library_list, str):
                conn_params['DefaultLibraries'] += library_list
            else:
                conn_params['DefaultLibraries'] += ','.join(library_list)

        return conn_params

    # To get new connection from Database
    def get_new_connection(self, conn_params):
        return pyodbc.connect("Driver=IBM i Access ODBC Driver; UNICODESQL=1; TRUEAUTOCOMMIT=1;", **conn_params)

    def create_cursor(self, name=None):
        cursor = self.connection.cursor()
        return DB2CursorWrapper(cursor, self.connection)

    def init_connection_state(self):
        pass

    def is_usable(self):
        try:
            # If connection is closed and unusable, a Programming error will result
            self.connection.cursor()
        except pyodbc.ProgrammingError:
            return False
        return True

    def _set_autocommit(self, autocommit):
        with self.wrap_database_errors:
            self.connection.autocommit = autocommit

    def close(self):
        if self.connection is not None:
            self.validate_thread_sharing()
            self.connection.close()
            self.connection = None

    def get_server_version(self):
        if not self.connection:
            self.cursor()
        return tuple(int(version) for version in
                     self.connection.server_info()[1].split("."))

    def schema_editor(self, *args, **kwargs):
        return DB2SchemaEditor(self, *args, **kwargs)


class DB2CursorWrapper:
    """
    This is the wrapper around pyodbc in order to support format parameter style
    pyodbc supports qmark, where as Django support format style,
    hence this conversion is required.
    """

    def __init__(self, cursor, conn):
        self.cursor = cursor
        self.conn = conn

    def __getattr__(self, attr):
        return getattr(self.cursor, attr)

    def __iter__(self):
        return iter(self.cursor)

    def __next__(self):
        return next(self.cursor)

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
                result = self.cursor.execute(operation, parameters)
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
                return self.cursor.executemany(operation, seq_parameters)
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
        self.cursor.execute(checkReorgSQL, parameters)
        res = self.cursor.fetchall()
        if res:
            for sName, tName in res:
                reorgSQL = '''CALL SYSPROC.ADMIN_CMD('REORG TABLE "%(sName)s"."%(tName)s"')''' % {
                    'sName': sName, 'tName': tName}
                reorgSQLs.append(reorgSQL)
            for sql in reorgSQLs:
                self.cursor.execute(sql)

    # Over-riding this method to modify result set containing datetime and time zone support is active
    def fetchone(self):
        row = self.cursor.fetchone()
        if row is None:
            return row
        else:
            return self._fix_return_data(row)

    # Over-riding this method to modify result set containing datetime and time zone support is active
    def fetchmany(self, size=0):
        rows = self.cursor.fetchmany(size)
        if rows is None:
            return rows
        else:
            return [self._fix_return_data(row) for row in rows]

    # Over-riding this method to modify result set containing datetime and time zone support is active
    def fetchall(self):
        rows = self.cursor.fetchall()
        if rows is None:
            return rows
        else:
            return [self._fix_return_data(row) for row in rows]

    # This method to modify result set containing datetime and time zone support is active
    def _fix_return_data(self, row):
        row = list(row)
        index = -1
        for value, desc in zip(row, self.cursor.description):
            index = index + 1
            if (desc[1] == pyodbc.DATETIME):
                if settings.USE_TZ and value is not None and timezone.is_naive(
                        value):
                    value = value.replace(tzinfo=timezone.utc)
                    row[index] = value
            if isinstance(value, str):
                row[index] = re.sub(r'[\x00]', '', value)
        return tuple(row)

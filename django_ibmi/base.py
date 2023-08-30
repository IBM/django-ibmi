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
from pprint import pprint

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


class DatabaseFeatures(BaseDatabaseFeatures):
    can_use_chunked_reads = True

    # Save point is supported by DB2.
    uses_savepoints = True

    # Custom query class has been implemented
    # django.db.backends.db2.query.query_class.DB2QueryClass
    uses_custom_query_class = True

    # transaction is supported by DB2
    supports_transactions = True

    # TODO: I don't think IBM i supports this
    supports_tablespaces = False

    implied_column_null = True

    interprets_empty_strings_as_nulls = False
    allows_primary_key_0 = True
    can_defer_constraint_checks = False
    supports_forward_references = False
    requires_rollback_on_dirty_transaction = True
    supports_regex_backreferencing = True
    supports_timezones = False
    has_bulk_insert = False
    has_select_for_update = True
    has_select_for_update_nowait = False
    has_select_for_update_skip_locked = False
    has_select_for_update_of = False
    supports_long_model_names = False
    can_distinct_on_fields = False
    supports_paramstyle_pyformat = False
    supports_sequence_reset = True
    can_return_id_from_insert = True
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

    bare_select_suffix = ' FROM SYSIBM.SYSDUMMY1'


class DatabaseValidation(BaseDatabaseValidation):
    # Need to do validation for IBM i and pyodbc version
    def validate_field(self, errors, opts, f):
        pass


class DatabaseWrapper(BaseDatabaseWrapper):
    """
    This is the base class for DB2 backend support for Django. The under lying
    wrapper is pyodbc.
    """

    vendor = 'db2'
    display_name = 'Db2 for i'

    data_types = {
        # DB2 Specific
        'AutoField':                    'INTEGER GENERATED BY DEFAULT AS IDENTITY '
                                        '(START WITH 1, INCREMENT BY 1, CACHE 10 ORDER)',
        # DB2 Specific
        'BigAutoField':                 'BIGINT GENERATED BY DEFAULT AS IDENTITY '
                                        '(START WITH 1, INCREMENT BY 1, CACHE 10 ORDER)',
        'CharField':                    'VARCHAR(%(max_length)s) ALLOCATE(%(max_length)s) CCSID 1208',
        'CommaSeparatedIntegerField':   'VARCHAR(%(max_length)s) ALLOCATE(%(max_length)s) CCSID 37',
        'DateField':                    'DATE',
        'DateTimeField':                'TIMESTAMP',
        'DecimalField':                 'DECIMAL(%(max_digits)s, %(decimal_places)s)',
        'FileField':                    'VARCHAR(%(max_length)s) ALLOCATE(%(max_length)s) CCSID 1208',
        'FilePathField':                'VARCHAR(%(max_length)s) ALLOCATE(%(max_length)s) CCSID 1208',
        'FloatField':                   'DOUBLE',
        'ImageField':                   'VARCHAR(%(max_length)s) ALLOCATE(%(max_length)s) CCSID 1208',
        'IntegerField':                 'INTEGER',
        'BigIntegerField':              'BIGINT',
        # TODO: I don't think this exists anymore, replaced with GenericIPAddressField
        'IPAddressField':               'VARCHAR(15) ALLOCATE(15) CCSID 37',
        'GenericIPAddressField':        'VARCHAR(39) ALLOCATE(39) CCSID 37',
        'ManyToManyField':              'VARCHAR(%(max_length)s) ALLOCATE(%(max_length)s) CCSID 1208',
        'OneToOneField':                'VARCHAR(%(max_length)s) ALLOCATE(%(max_length)s) CCSID 1208',
        # TODO: I don't think this exists anymore
        'PhoneNumberField':             'VARCHAR(16) CCSID 37 ALLOCATE(16)',
        'SlugField':                    'VARCHAR(%(max_length)s) ALLOCATE(%(max_length)s) CCSID 1208',
        'SmallIntegerField':            'SMALLINT',
        'TextField':                    'CLOB CCSID 1208',
        'TimeField':                    'TIME',
        'USStateField':                 'CHAR(2) CCSID 37',
        'URLField':                     'VARCHAR(%(max_length)s) ALLOCATE(%(max_length)s) CCSID 1208',
        'XMLField':                     'XML',
        'BinaryField':                  'BLOB',
        'UUIDField':                    'CHAR(32) CCSID 37',
        'DurationField':                'DOUBLE',
        'BooleanField':                 'SMALLINT',
        'NullBooleanField':             'SMALLINT',
        'PositiveIntegerField':         'INTEGER',
        'PositiveSmallIntegerField':    'SMALLINT',

    }

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

    # NOTE: These were added in Django 1.7 (commit 28a02259cb9) and assumed to
    #       exist for all backends, but there's nothing mentioning this in the
    #       docs that it is needed. Without it, if you try to use a filter you'll
    #       get an AttributeError in django/db/models/lookups.py

    # The patterns below are used to generate SQL pattern lookup clauses when
    # the right-hand side of the lookup isn't a raw string (it might be an expression
    # or the result of a bilateral transformation).
    # In those cases, special characters for LIKE operators (e.g. \, *, _) should be
    # escaped on database side.
    #
    # Note: we use str.format() here for readability as '%' is used as a wildcard for
    # the LIKE operator.
    pattern_esc = r"REPLACE(REPLACE(REPLACE({}, '\\', '\\\\'), '%%', '\%%'), '_', '\_')"
    pattern_ops = {
        # TODO: This was copied from MySQL backend, make sure it's correct for us
        # TODO: Figure out how to do case-sensitive and case-insensitive matches
        'contains': "LIKE CONCAT('%%', {}, '%%')",
        'icontains': "LIKE CONCAT('%%', {}, '%%')",
        'startswith': "LIKE CONCAT({}, '%%')",
        'istartswith': "LIKE CONCAT({}, '%%')",
        'endswith': "LIKE CONCAT('%%', {})",
        'iendswith': "LIKE CONCAT('%%', {})",
    }

    data_type_check_constraints = {
        'BooleanField': '%(qn_column)s IN (0,1)',
        'NullBooleanField': '(%(qn_column)s IN (0,1)) OR (%(qn_column)s IS NULL)',
        'PositiveIntegerField': '%(qn_column)s >= 0',
        'PositiveSmallIntegerField': '%(qn_column)s >= 0',
    }

    Database = pyodbc

    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    validation_class = DatabaseValidation
    ops_class = DatabaseOperations

    # Constructor of DB2 backend support. Initializing all other classes.
    # NOTE: Unneeded. Django does this for us using the *_class attributes above
    #def __init__(self, *args, **kwargs):
    #    super().__init__(*args, **kwargs)
    #    self.client = DatabaseClient(self)
    #    self.creation = DatabaseCreation(self)
    #    self.features = DatabaseFeatures(self)
    #    self.introspection = DatabaseIntrospection(self)
    #    self.ops = DatabaseOperations(self)
    #    self.validation = DatabaseValidation(self)

    #    self.data_types = self.creation.data_types
    #    self.data_type_check_constraints = self.creation.data_type_check_constraints

    # Method to check if connection is live or not.
    def __is_connection(self):
        return self.connection is not None

    # To get dict of connection parameters
    def get_connection_params(self):
        # we get passed this:
        """({'ATOMIC_REQUESTS': False,
            'AUTOCOMMIT': True,
            'CONN_MAX_AGE': 0,
            'ENGINE': 'django_ibmi',
            'HOST': '',
            'NAME': 'oss75dev',
            'OPTIONS': {},
            'PASSWORD': 'xxxxxxxx',
            'PORT': '',
            'TEST': {'CHARSET': None, 'COLLATION': None, 'MIRROR': None, 'NAME': None},
            'TIME_ZONE': None,
            'USER': 'kadler'},
            'default')"""

        settings_dict = self.settings_dict
        conn_params = {}

        name = settings_dict['NAME']
        if name == '':
            raise ImproperlyConfigured("settings.DATABASES is improperly configured. Please supply the NAME value.")
        
        if name is not None and len(name) > self.ops.max_name_length():
            raise ImproperlyConfigured(f"The database name {name} is longer than the max schmea name")
        
        conn_params['schema'] = name

        for attr in ('USER', 'PASSWORD', 'HOST'):
            if not settings_dict[attr]:
                raise ImproperlyConfigured(f"settings.DATABASES is improperly configured. Please supply the {attr} value.")

        conn_params['user'] = settings_dict['USER']
        conn_params['password'] = settings_dict['PASSWORD']
        conn_params['system'] = settings_dict['HOST']

        return conn_params


    # To get new connection from Database
    def get_new_connection(self, conn_params):
        # https://docs.djangoproject.com/en/4.2/ref/settings/#std-setting-DATABASES

        # from pprint import pprint
        # print('get_new_connection')
        # print('conn_params')
        # pprint(conn_params)
        # print('settings_dict')
        # pprint(self.settings_dict)
        # exit(1)

        conn_str = "DRIVER={IBM i Access ODBC Driver};CCSID=1208;UNICODESQL=1;DBQ=,;"
        
        conn_str += "SYSTEM={c[system]};UID={c[user]};PWD={c[password]}".format(c=conn_params)

        conn = pyodbc.connect(conn_str)
        conn.setencoding(encoding='utf-8')

        schema = conn_params["schema"]
        if schema is not None:
            schema = self.ops.quote_name(schema)
            conn.execute(f"SET CURRENT SCHEMA {schema}")
        
        return conn

    def create_cursor(self, name=None):
        return DB2CursorWrapper(self.connection)

    def init_connection_state(self):
        pass

    def is_usable(self):
        try:
            self.connection.cursor()
        except pyodbc.ProgrammingError:
            return False

        return True

    def _set_autocommit(self, autocommit):
        self.connection.autocommit = autocommit

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

    def __init__(self, connection):
        self.cursor = connection.cursor()

    # Forward all attributes to the pyodbc cursor other than those we override
    def __getattr__(self, attr):
        return getattr(self.cursor, attr)

    def __iter__(self):
        return self
    
    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

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

    # TODO: We could have a set of fixup functions and noop functions and based
    # on whether USE_TZ is set and we have any timezone columns, we can enable
    # or disable the fixup functions.

    # Over-riding this method to modify SQLs which contains format parameter
    # to qmark.
    def execute(self, operation, parameters=()):
        operation = str(operation)


        try:
            # TODO: I don't think this is applicable on IBM i
            dbms_name = 'AS'
            doReorg = 1 if operation.find('ALTER TABLE') == 0 and dbms_name != 'DB2' else 0
            if operation.count("db2regexExtraField(%s)") > 0:
                operation = operation.replace("db2regexExtraField(%s)", "")
                operation = operation % parameters
                parameters = ()
            if operation.count("%s") > 0:
                operation = operation % (tuple("?" * operation.count("%s")))
            parameters = self._format_parameters(parameters)

            try:
                print(operation)
                self.cursor.execute(operation, parameters)
                if doReorg == 1:
                    return self._reorg_tables()
            except IntegrityError as e:
                raise utils.IntegrityError(*e.args) from e

            except ProgrammingError as e:
                raise utils.ProgrammingError(*e.args) from e

            except DatabaseError as e:
                print(operation)
                pprint(parameters)
                raise utils.DatabaseError(*e.args) from e

        except TypeError:
            return None

    # TODO: Don't support this, since pyodbc just uses loops on multiple calls to execute
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
        # TODO: I don't think IBM i supports this, certainly ADMINTABINFO doesn't exist
        return

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

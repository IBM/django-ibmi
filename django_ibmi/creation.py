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

import sys
try:
    from django.db.backends.creation import BaseDatabaseCreation
except ImportError:
    from django.db.backends.base.creation import BaseDatabaseCreation
from django.core.management import call_command
from django.db.backends.utils import truncate_name


TEST_DBNAME_PREFIX = 'test_'


class DatabaseCreation (BaseDatabaseCreation):
    psudo_column_prefix = 'psudo_'

    def sql_indexes_for_field(self, model, f, style):
        """Return the CREATE INDEX SQL statements for a single model field"""
        output = []
        qn = self.connection.ops.quote_name
        max_name_length = self.connection.ops.max_name_length()
        # ignore tablespace information
        tablespace_sql = ''
        i = 0
        # TODO: Check this for IBM i compatibility
        dbms_name = 'AS'

        if 'DB2' not in dbms_name or dbms_name != 'DB2':
            if len(model._meta.unique_together_index) != 0:
                for unique_together_index in model._meta.unique_together_index:
                    i = i + 1
                    column_list = []
                    for column in unique_together_index:
                        for local_field in model._meta.local_fields:
                            if column == local_field.name:
                                column_list.extend([local_field.column])

                    self.__add_psudokey_column(style, self.connection.cursor(
                    ), model._meta.db_table, model._meta.pk.attname, column_list)
                    column_list.extend([truncate_name(
                        "%s%s" % (self.psudo_column_prefix, "_".join(column_list)), max_name_length)])
                    output.extend([style.SQL_KEYWORD('CREATE UNIQUE INDEX') + ' ' +
                                   style.SQL_TABLE(qn('db2_%s_%s' % (model._meta.db_table, i))) + ' ' +
                                   style.SQL_KEYWORD('ON') + ' ' +
                                   style.SQL_TABLE(qn(model._meta.db_table)) + ' ' +
                                   '( %s )' % ", ".join(column_list) + ' ' +
                                   '%s;' % tablespace_sql])
                model._meta.unique_together_index = []

            if f.unique_index:
                column_list = []
                column_list.extend([f.column])
                self.__add_psudokey_column(style, self.connection.cursor(
                ), model._meta.db_table, model._meta.pk.attname, column_list)
                cisql = 'CREATE UNIQUE INDEX'
                output.extend([style.SQL_KEYWORD(cisql) + ' ' +
                               style.SQL_TABLE(qn('%s_%s' % (model._meta.db_table, f.column))) + ' ' +
                               style.SQL_KEYWORD('ON') + ' ' +
                               style.SQL_TABLE(qn(model._meta.db_table)) + ' ' +
                               "(%s, %s )" % (style.SQL_FIELD(qn(f.column)),
                                              style.SQL_FIELD(qn(truncate_name((self.psudo_column_prefix + f.column),
                                                                               max_name_length)))) +
                               "%s;" % tablespace_sql])
                return output

        if f.db_index and not f.unique:
            cisql = 'CREATE INDEX'
            output.extend([style.SQL_KEYWORD(cisql) + ' ' +
                           style.SQL_TABLE(qn('%s_%s' % (model._meta.db_table, f.column))) + ' ' +
                           style.SQL_KEYWORD('ON') + ' ' +
                           style.SQL_TABLE(qn(model._meta.db_table)) + ' ' +
                           "(%s)" % style.SQL_FIELD(qn(f.column)) +
                           "%s;" % tablespace_sql])

        return output

    #def create_test_db(self, *args, **kwargs):
    #    self.connection._nodb_connection
    #    raise Exception
    
    def _create_test_db(self, verbosity, autoclobber, keepdb=False):
        test_database_name = self._get_test_db_name()
        test_database_name_quoted = self.connection.ops.quote_name(test_database_name)
        create_schema_sql = f"CREATE SCHEMA {test_database_name_quoted}"
        with self._nodb_connection.cursor() as cursor:
            try:
                cursor.execute(create_schema_sql)
            except Exception as e:
                if keepdb:
                    return test_database_name

                self.log('Got an error creating the test database: %s' % e)
                from pprint import pprint
                pprint(e)
                if not autoclobber:
                    confirm = input(
                        "Type 'yes' if you would like to try deleting the test "
                        "database '%s', or 'no' to cancel: " % test_database_name)
                if autoclobber or confirm == 'yes':
                    try:
                        if verbosity >= 1:
                            self.log('Destroying old test database for alias %s...' % (
                                self._get_database_display_str(verbosity, test_database_name),
                            ))
                        cursor.execute(f'DROP SCHEMA {test_database_name_quoted} CASCADE')
                        cursor.execute(create_schema_sql)
                    except Exception as e:
                        self.log('Got an error recreating the test database: %s' % e)
                        pprint(e)
                        sys.exit(2)
                else:
                    self.log('Tests cancelled.')
                    sys.exit(1)

        return test_database_name


    def _destroy_test_db(self, test_database_name, verbosity):
        test_database_name_quoted = self.connection.ops.quote_name(test_database_name)
        with self.connection._nodb_connection.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA {test_database_name_quoted} CASCADE')

    # Method to create and return test database, before creating test database it takes confirmation from user.
    # If test database already exists then it takes confirmation from user to recreate that database .
    # If create test database not supported in current scenario then it takes confirmation from user to use settings file's
    # database name as test database
    # For Jython this method prepare the settings file's database. First it drops the tables from the database,then create
    # tables on the basis of installed models.
    def create_test_db_old(self, verbosity=0, autoclobber=False, keepdb=False, serialize=False):
        # For testing
        autoclobber = True

        kwargs = self.__create_test_kwargs()
        old_database = kwargs['database']
        max_db_name_length = self.connection.ops.max_db_name_length()
        kwargs['database'] = truncate_name(
            "%s%s" % (TEST_DBNAME_PREFIX, old_database), max_db_name_length)
        kwargsKeys = kwargs.keys()
        if (kwargsKeys.__contains__('port') and
                kwargsKeys.__contains__('host')):
            kwargs['dsn'] = "DATABASE=%s;HOSTNAME=%s;PORT=%s;PROTOCOL=TCPIP;" % (
                kwargs.get('dbname'),
                kwargs.get('host'),
                kwargs.get('port')
            )
        else:
            kwargs['dsn'] = ''
        if kwargsKeys.__contains__('port'):
            del kwargs['port']

        if not autoclobber:
            confirm = input("Wants to create %s as test database. Type yes to create it else type no" % (
                kwargs.get('database')))
        if autoclobber or confirm == 'yes':
            try:
                if verbosity > 1:
                    print("Creating Test Database %s" %
                          (kwargs.get('database')))
                # comment out for now, will be fixed with rest of module
                # pyodbc.createdb( **kwargs )
            except Exception as inst:
                message = repr(inst)
                if message.find('Not supported:') != -1:
                    if not autoclobber:
                        confirm = input("Not able to create test database, %s. Type yes to use %s as test database, "
                                        "or no to exit" % (message.split(":")[1], old_database))
                    else:
                        confirm = \
                            input("Not able to create test database, %s. Type yes to use %s as test database, "
                                  "or no to exit" % (message.split(":")[1], old_database))

                    if autoclobber or confirm == 'yes':
                        kwargs['database'] = old_database
                        self.__clean_up(self.connection.cursor())
                        self.connection._commit()
                        self.connection.close()
                    else:
                        print("Tests cancelled")
                        sys.exit(1)
                else:
                    sys.stderr.write(
                        "Error occurred during creation of test database: %s" % message)
                    index = message.find('SQLCODE')
                    if (message != '') & (index != -1):
                        error_code = message[(index + 8): (index + 13)]
                        if error_code != '-1005':
                            print("Tests cancelled")
                            sys.exit(1)
                        else:
                            if not autoclobber:
                                confirm = input(
                                    "\nTest database: %s already exist. Type yes to recreate it, or no to exit"
                                    % (kwargs.get('database')))
                            else:
                                confirm = input(
                                    "\nTest database: %s already exist. Type yes to recreate it, or no to exit"
                                    % (kwargs.get('database')))
                            if autoclobber or confirm == 'yes':
                                if verbosity > 1:
                                    print(("Recreating Test Database %s" % kwargs.get('database')))
                                # comment out for now, will be fixed with rest of module
                                # pyodbc.recreatedb( **kwargs )
                            else:
                                print("Tests cancelled.")
                                sys.exit(1)
        else:
            confirm = input(
                "Wants to use %s as test database, Type yes to use it as test database or no to exit" % (old_database))
            if confirm == 'yes':
                kwargs['database'] = old_database
                self.__clean_up(self.connection.cursor())
                self.connection._commit()
                self.connection.close()
            else:
                sys.exit(1)

        test_database = kwargs.get('database')
        if verbosity > 1:
            print("Preparing Database...")

        self.connection.settings_dict['NAME'] = test_database
        self.connection.settings_dict['PCONNECT'] = False
        # Confirm the feature set of the test database
        call_command('migrate', database=self.connection.alias,
                     verbosity=verbosity, interactive=False)
        # We need to then do a flush to ensure that any data installed by custom SQL has been removed.
        call_command('flush', database=self.connection.alias,
                     verbosity=verbosity, interactive=False)
        return test_database

    # Method to destroy database. For Jython nothing is getting done over here.
    def destroy_test_db_old(self, old_database_name, verbosity=0):
        print("Destroying Database...")
        kwargs = self.__create_test_kwargs()
        if old_database_name != kwargs.get('database'):
            kwargsKeys = kwargs.keys()
            if (kwargsKeys.__contains__('port') and
                    kwargsKeys.__contains__('host')):
                kwargs['dsn'] = "DATABASE=%s;HOSTNAME=%s;PORT=%s;PROTOCOL=TCPIP;" % (
                    kwargs.get('database'),
                    kwargs.get('host'),
                    kwargs.get('port')
                )
            else:
                kwargs['dsn'] = ''
            if kwargsKeys.__contains__('port'):
                del kwargs['port']
            if verbosity > 1:
                print("Droping Test Database %s" % (kwargs.get('database')))
            # comment out for now, will be fixed with rest of module
            # pyodbc.dropdb( **kwargs )

        self.connection.settings_dict['NAME'] = old_database_name
        self.connection.settings_dict['PCONNECT'] = True
        return old_database_name

    # As DB2 does not allow to insert NULL value in UNIQUE col, hence modifing model.
    def sql_create_model(self, model, style, known_models=set()):
        if getattr(self.connection.connection, dbms_name) != 'DB2':
            model._meta.unique_together_index = []
            temp_changed_uvalues = []
            temp_unique_together = model._meta.unique_together
            for i in range(len(model._meta.local_fields)):
                model._meta.local_fields[i].unique_index = False
                if model._meta.local_fields[i]._unique and model._meta.local_fields[i].null:
                    model._meta.local_fields[i].unique_index = True
                    model._meta.local_fields[i]._unique = False
                    temp_changed_uvalues.append(i)

                if len(model._meta.unique_together) != 0:
                    for unique_together in model._meta.unique_together:
                        if model._meta.local_fields[i].name in unique_together:
                            if model._meta.local_fields[i].null:
                                unique_list = list(model._meta.unique_together)
                                unique_list.remove(unique_together)
                                model._meta.unique_together = tuple(
                                    unique_list)
                                model._meta.unique_together_index.append(
                                    unique_together)
            sql, references = super().sql_create_model(model, style,
                                                       known_models)

            for i in temp_changed_uvalues:
                model._meta.local_fields[i]._unique = True
            model._meta.unique_together = temp_unique_together
            return sql, references
        else:
            return super().sql_create_model(model, style, known_models)

    # Private method to clean up database.
    def __clean_up(self, cursor):
        tables = self.connection.introspection.django_table_names(
            only_existing=True)
        for table in tables:
            sql = "DROP TABLE %s" % self.connection.ops.quote_name(table)
            cursor.execute(sql)
        cursor.close()

    # Private method to alter a table with adding psudokey column
    def __add_psudokey_column(self, style, cursor, table_name, pk_name, column_list):
        qn = self.connection.ops.quote_name
        max_name_length = self.connection.ops.max_name_length()

        sql = style.SQL_KEYWORD('ALTER TABLE ') + \
            style.SQL_TABLE(qn(table_name)) + \
            style.SQL_KEYWORD(' ADD COLUMN ') + \
            style.SQL_FIELD(qn(truncate_name("%s%s" % (self.psudo_column_prefix, "_".join(column_list)), max_name_length))) + \
            style.SQL_KEYWORD(' GENERATED ALWAYS AS( CASE WHEN ') + \
            style.SQL_FIELD("%s %s" % (" IS NULL OR ".join(column_list), 'IS NULL THEN ')) + \
            style.SQL_FIELD(qn(pk_name)) + \
            style.SQL_KEYWORD(' END ) ;')
        cursor.execute('SET INTEGRITY FOR ' +
                       style.SQL_TABLE(qn(table_name)) + ' OFF CASCADE DEFERRED;')
        cursor.execute(sql)
        cursor.execute('SET INTEGRITY FOR ' +
                       style.SQL_TABLE(table_name) + ' IMMEDIATE CHECKED;')
        cursor.close()

    # private method to create dictionary of login credentials for test database
    def __create_test_kwargs(self):

        if(isinstance(self.connection.settings_dict['NAME'], str) and
                (self.connection.settings_dict['NAME'] != '')):
            database = self.connection.settings_dict['NAME']
        else:
            from django.core.exceptions import ImproperlyConfigured
            raise ImproperlyConfigured("the database Name doesn't exist")
        database_user = self.connection.settings_dict['USER']
        database_pass = self.connection.settings_dict['PASSWORD']
        database_host = self.connection.settings_dict['HOST']
        database_port = self.connection.settings_dict['PORT']
        self.connection.settings_dict['SUPPORTS_TRANSACTIONS'] = True

        kwargs = {}
        kwargs['database'] = database
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

        return kwargs

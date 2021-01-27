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
This module implements command line interface for DB2 through Django.
"""
from distutils import util

import pyodbc
from django.core.exceptions import ImproperlyConfigured

try:
    from django.db.backends import BaseDatabaseClient
except ImportError:
    from django.db.backends.base.client import BaseDatabaseClient
import os


class DatabaseClient(BaseDatabaseClient):

    # Over-riding base method to provide shell support for DB2 through Django.
    def runshell(self):
        settings_dict = self.connection.settings_dict
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

        allowed_opts = {'system', 'user', 'password', 'autocommit', 'readonly','timeout', 'database', 'use_system_naming',
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
            if isinstance(conn_params["DefaultLibraries"], str):
                conn_params['DefaultLibraries'] += \
                    conn_params.pop('library_list', '')
            else:
                conn_params['DefaultLibraries'] += ','.join(
                    conn_params.pop('library_list', ''))

        if os.name == 'nt':
            cnxn = pyodbc.connect("Driver=IBM i Access ODBC Driver; UNICODESQL=1; TRUEAUTOCOMMIT=1;", **conn_params)
            cursor = cnxn.cursor()
            while True:
                try:
                    query = input("SQL> ")
                    if query == "exit":
                        break
                    cursor.execute(query)
                    try:
                        print(cursor.fetchall())
                    except pyodbc.ProgrammingError:
                        pass
                except ConnectionError as e:
                    cursor.close()
                    cnxn.close()
                    print(e)
                    break
                except KeyboardInterrupt:
                    cursor.close()
                    cnxn.close()
                    break
            cursor.close()
            cnxn.close()

        else:
            args = ['%s -v %s %s %s' %
                    ('isql', settings_dict['NAME'], settings_dict['USER'], settings_dict['PASSWORD'])]
            try:
                os.subprocess.call(args, shell=True)
            except KeyboardInterrupt:
                pass

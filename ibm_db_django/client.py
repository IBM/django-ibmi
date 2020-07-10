# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                    |
# |                                                                          |
# | (C) Copyright IBM Corporation 2009-2018.                                 |
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

import pyodbc
from django.db.backends.base.client import BaseDatabaseClient
import os
import subprocess


class DatabaseClient(BaseDatabaseClient):
    def runshell(self):
        settings_dict = self.connection.settings_dict
        user = settings_dict['USER']
        password = settings_dict['PASSWORD']
        system = settings_dict['NAME']
        options = settings_dict['OPTIONS']
        driver = options['driver']
        if os.name == 'nt':
            cnxn = pyodbc.connect(
                f"SYSTEM={system};UID={user};PWD={password};DRIVER={driver}")
            cursor = cnxn.cursor()
            while True:
                query = input("SQL> ")
                if query == "exit":
                    break
                result = None
                try:
                    result = cursor.execute(query)
                except ConnectionError as e:
                    print("An error occurred on connection")
                except KeyboardInterrupt:
                    pass

                try:
                    print(result.fetchall())
                except pyodbc.ProgrammingError as e:
                    pass

            cursor.close()
            cnxn.close()
        else:
            args = ['%s -v %s %s %s' %
                    ('isql', system, user, password)]
            try:
                subprocess.call(args, shell=True)
            except KeyboardInterrupt:
                pass

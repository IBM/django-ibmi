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
from collections import namedtuple

from django.db.backends.base.introspection import BaseDatabaseIntrospection, TableInfo

# TODO fix pyodbc access in Introspection when doing rest of module
# after fix to DatabaseWrapper and CursorWrapper

class DatabaseIntrospection(BaseDatabaseIntrospection):

    """
    This is the class where database metadata information can be generated.
    """

    data_types_reverse = {
        # TODO define reverse data types
    }

    def get_field_type(self, data_type, description):
        return super().get_field_type(data_type, description)

    # Converting table name to lower case.
    # TODO: Replaced with identifier_converter in Django 2.2
    #       However, we shouldn't be doing this anyway because identifiers are case-senstive when quoted
    #def table_name_converter(self, name):
    #    return name.lower()

    # Getting the list of all tables, which are present under current schema.
    def get_table_list(self, cursor):
        cursor.execute("""SELECT
            TABLE_NAME,
            CASE 
                WHEN TABLE_TYPE IN ('P', 'T') THEN 't'
                ELSE 'v'
            END
        FROM SYSTABLES
        WHERE SYSTEM_TABLE = 'N'
        """)
        return [TableInfo(*row) for row in cursor.fetchall()]

        #from pprint import pprint
        # print(type(cursor))
        # print(dir(cursor))
        # exit()
        # pprint(cursor)
        #TableInfo = namedtuple('TableInfo', ['name', 'type'])
        #table_list = []
        # for row in cursor.tables():
        #     table_list.append(TableInfo(row[2], 't'))
        #return [TableInfo(row[2], 't') for row in cursor.tables()]

    # Generating a dictionary for foreign key details, which are present under current schema.
    def get_relations(self, cursor, table_name):
        relations = {}
        schema = cursor.connection.get_current_schema()
        for fk in cursor.connection.foreign_keys(True, schema, table_name):
            relations[self.__get_col_index(cursor, schema, table_name, fk['FKCOLUMN_NAME'])] = (
                self.__get_col_index(cursor, schema, fk['PKTABLE_NAME'], fk['PKCOLUMN_NAME']), fk['PKTABLE_NAME'].lower())

        return relations

    # Private method. Getting Index position of column by its name
    def __get_col_index(self, cursor, schema, table_name, col_name):
        for col in cursor.connection.columns(schema, table_name, [col_name]):
            return col['ORDINAL_POSITION'] - 1

    def get_key_columns(self, cursor, table_name):
        relations = []
        schema = cursor.connection.get_current_schema()
        for fk in cursor.connection.foreign_keys(True, schema, table_name):
            relations.append((fk['FKCOLUMN_NAME'].lower(
            ), fk['PKTABLE_NAME'].lower(), fk['PKCOLUMN_NAME'].lower()))

        return relations

    # Getting list of indexes associated with the table provided.
    def get_indexes(self, cursor, table_name):
        indexes = {}
        # To skip indexes across multiple fields
        multifield_indexSet = set()
        schema = cursor.connection.get_current_schema()
        all_indexes = cursor.connection.indexes(True, schema, table_name)
        for index in all_indexes:
            if (index['ORDINAL_POSITION'] is not None) and (index['ORDINAL_POSITION'] == 2):
                multifield_indexSet.add(index['INDEX_NAME'])

        for index in all_indexes:
            temp = {}
            if index['INDEX_NAME'] in multifield_indexSet:
                continue

            if index['NON_UNIQUE']:
                temp['unique'] = False
            else:
                temp['unique'] = True
            temp['primary_key'] = False
            indexes[index['COLUMN_NAME'].lower()] = temp

        for index in cursor.connection.primary_keys(True, schema, table_name):
            indexes[index['COLUMN_NAME'].lower()]['primary_key'] = True

        return indexes

    # Getting the description of the table.
    def get_table_description(self, cursor, table_name):
        description = []
        schema = cursor.connection.get_current_schema()

        sql = "SELECT TYPE FROM QSYS2.SYSTABLES WHERE TABLE_SCHEMA='%(schema)s' AND TABLE_NAME='%(table)s'" % {
            'schema': schema, 'table': table_name}
        
        cursor.execute(sql)
        table_type = cursor.fetchone()[0]

        if table_type != 'X':
            cursor.execute(
                "SELECT * FROM %s FETCH FIRST 1 ROWS ONLY" % qn(table_name))
            for desc in cursor.description:
                description.append([desc[0].lower(), ] + desc[1:])
        return description

    def get_constraints(self, cursor, table_name):
        constraints = {}
        schema = cursor.connection.get_current_schema()

        sql = "SELECT CONSTRAINT_NAME, COLUMN_NAME FROM QSYS2.SYSCSTCOL WHERE TABLE_SCHEMA='%(schema)s' AND " \
                "TABLE_NAME='%(table)s'" % {'schema': schema, 'table': table_name}

        cursor.execute(sql)
        for constname, colname in cursor.fetchall():
            if constname not in constraints:
                constraints[constname] = {
                    'columns': [],
                    'primary_key': False,
                    'unique': False,
                    'foreign_key': None,
                    'check': True,
                    'index': False
                }
            constraints[constname]['columns'].append(colname.lower())

        sql = "SELECT KEYCOL.CONSTRAINT_NAME, KEYCOL.COLUMN_NAME FROM QSYS2.SYSKEYCST KEYCOL INNER JOIN " \
                "QSYS2.SYSCST TABCONST ON KEYCOL.CONSTRAINT_NAME=TABCONST.CONSTRAINT_NAME WHERE TABCONST.TABLE_SCHEMA=" \
                "'%(schema)s' and TABCONST.TABLE_NAME='%(table)s' " \
                "and TABCONST.TYPE='U'" % {'schema': schema, 'table': table_name}

        cursor.execute(sql)
        for constname, colname in cursor.fetchall():
            if constname not in constraints:
                constraints[constname] = {
                    'columns': [],
                    'primary_key': False,
                    'unique': True,
                    'foreign_key': None,
                    'check': False,
                    'index': True
                }
            constraints[constname]['columns'].append(colname.lower())

        for pkey in cursor.connection.primary_keys(None, schema, table_name):
            if pkey['PK_NAME'] not in constraints:
                constraints[pkey['PK_NAME']] = {
                    'columns': [],
                    'primary_key': True,
                    'unique': False,
                    'foreign_key': None,
                    'check': False,
                    'index': True
                }
            constraints[pkey['PK_NAME']]['columns'].append(
                pkey['COLUMN_NAME'].lower())

        for fk in cursor.connection.foreign_keys(True, schema, table_name):
            if fk['FK_NAME'] not in constraints:
                constraints[fk['FK_NAME']] = {
                    'columns': [],
                    'primary_key': False,
                    'unique': False,
                    'foreign_key': (fk['PKTABLE_NAME'].lower(), fk['PKCOLUMN_NAME'].lower()),
                    'check': False,
                    'index': False
                }
            constraints[fk['FK_NAME']]['columns'].append(
                fk['FKCOLUMN_NAME'].lower())
            if fk['PKCOLUMN_NAME'].lower() not in constraints[fk['FK_NAME']]['foreign_key']:
                fkeylist = list(constraints[fk['FK_NAME']]['foreign_key'])
                fkeylist.append(fk['PKCOLUMN_NAME'].lower())
                constraints[fk['FK_NAME']]['foreign_key'] = tuple(fkeylist)

        for index in cursor.connection.indexes(True, schema, table_name):
            if index['INDEX_NAME'] not in constraints:
                constraints[index['INDEX_NAME']] = {
                    'columns': [],
                    'primary_key': False,
                    'unique': False,
                    'foreign_key': None,
                    'check': False,
                    'index': True
                }
            elif constraints[index['INDEX_NAME']]['unique']:
                continue
            elif constraints[index['INDEX_NAME']]['primary_key']:
                continue
            constraints[index['INDEX_NAME']]['columns'].append(
                index['COLUMN_NAME'].lower())
        return constraints

    def get_sequences(self, cursor, table_name, table_fields=()):
        from django.db import models

        seq_list = []
        for f in table_fields:
            if isinstance(f, models.AutoField):
                seq_list.append({'table': table_name, 'column': f.column})
                break
        return seq_list

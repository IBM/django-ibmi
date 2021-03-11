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
from django.db import models
from django.db.backends.base.introspection import BaseDatabaseIntrospection, FieldInfo


class DatabaseIntrospection(BaseDatabaseIntrospection):

    """
    This is the class where database metadata information can be generated.
    """

    data_types_reverse = {
        # TODO define reverse data types
    }

    # Converting table name to lower case.
    def identifier_converter(self, name):
        return name.lower()

    # Getting the list of all tables, which are present under current schema.
    def get_table_list(self, cursor):
        TableInfo = namedtuple('TableInfo', ['name', 'type'])
        table_list = []
        for table in cursor.cursor.tables(schema=self.connection.get_current_schema()):
            table_list.append(TableInfo(table.table_name.lower(), 't'))
        return table_list

    # Generating a dictionary for foreign key details, which are present under current schema.
    def get_relations(self, cursor, table_name):
        relations = {}
        schema = self.connection.get_current_schema()
        for fk in cursor.cursor.foreign_keys(schema=schema, table=table_name):
            relations[self.__get_col_index(cursor, schema, table_name, fk.fkcolumn_name)] = (
                self.__get_col_index(cursor, schema, fk.pktable_name, fk.pkcolumn_name), fk.pktable_name.lower())

        return relations

    # Private method. Getting Index position of column by its name
    def __get_col_index(self, cursor, schema, table_name, col_name):
        for col in cursor.cursor.columns(schema=schema, table=table_name, column=col_name):
            return col.ordinal_position - 1

    def get_key_columns(self, cursor, table_name):
        relations = []
        schema = self.connection.get_current_schema()
        for fk in cursor.cursor.foreign_keys(schema=schema, table=table_name):
            relations.append((fk.fkcolumn_name.lower(), fk.pktable_name.lower(), fk.pkcolumn_name.lower()))

        return relations

    # Getting the description of the table.
    def get_table_description(self, cursor, table_name):
        qn = self.connection.ops.quote_name
        schema = self.connection.get_current_schema()
        sql = "SELECT TYPE FROM QSYS2.SYSTABLES WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'" % \
              (schema.upper(), table_name.upper())
        cursor.execute(sql)
        table_type = cursor.fetchone()[0]

        field_map = {str(line.column_name): [line.data_type, str(line.column_def).replace("'", "")]
                     for line in cursor.columns(schema=schema.upper(), table=table_name.upper())}

        if table_type != 'X':
            cursor.execute(
                "SELECT * FROM %s FETCH FIRST 1 ROWS ONLY" % qn(table_name))
        return [
            FieldInfo(
                line[0],
                field_map[str(line[0])][0],
                *line[2:],
                field_map[str(line[0])][1]
            )
            for line in cursor.description
        ]

    def get_constraints(self, cursor, table_name):
        constraints = {}
        schema = self.connection.get_current_schema()
        sql = "SELECT CONSTRAINT_NAME, COLUMN_NAME FROM QSYS2.SYSCSTCOL WHERE TABLE_SCHEMA='%s' AND " \
              "TABLE_NAME='%s'" % (schema.upper(), table_name.upper())
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
        sql = "SELECT KEYCOL.CONSTRAINT_NAME, KEYCOL.COLUMN_NAME FROM " \
              "QSYS2.SYSKEYCST KEYCOL INNER JOIN QSYS2.SYSCST TABCONST ON " \
              "KEYCOL.CONSTRAINT_NAME=TABCONST.CONSTRAINT_NAME WHERE " \
              "TABCONST.TABLE_SCHEMA='%s' and " \
              "TABCONST.TABLE_NAME='%s' and " \
              "TABCONST.TYPE='U'" % (schema.upper(), table_name.upper())
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

        for pkey in cursor.cursor.primaryKeys(
                schema=schema, table=table_name.upper()):
            if pkey.pk_name not in constraints:
                constraints[pkey.pk_name] = {
                    'columns': [],
                    'primary_key': True,
                    'unique': False,
                    'foreign_key': None,
                    'check': False,
                    'index': True
                }
            constraints[pkey.pk_name]['columns'].append(
                pkey.column_name.lower())

        for fk in cursor.cursor.foreignKeys(schema=schema, table=table_name.upper()):
            if fk.fk_name not in constraints:
                constraints[fk.fk_name] = {
                    'columns': [],
                    'primary_key': False,
                    'unique': False,
                    'foreign_key': (fk.pktable_name.lower(),
                                    fk.pkcolumn_name.lower()),
                    'check': False,
                    'index': False
                }
            constraints[fk.fk_name]['columns'].append(
                fk.fkcolumn_name.lower())
            if fk.pkcolumn_name.lower() not in \
                    constraints[fk.fk_name]['foreign_key']:
                fkeylist = list(constraints[fk.fk_name]['foreign_key'])
                fkeylist.append(fk.pkcolumn_name.lower())
                constraints[fk.fk_name]['foreign_key'] = fkeylist

        for index in cursor.cursor.statistics(
                unique=True, schema=schema, table=table_name.upper()):
            if index.column_name:
                if index.index_name not in constraints:
                    constraints[index.index_name] = {
                        'columns': [],
                        'primary_key': False,
                        'unique': False,
                        'foreign_key': None,
                        'check': False,
                        'index': True
                    }
                elif constraints[index.index_name]['unique']:
                    continue
                elif constraints[index.index_name]['primary_key']:
                    continue
                constraints[index.index_name]['columns'].append(
                    index.column_name.lower())
        return constraints

    def get_sequences(self, cursor, table_name, table_fields=()):
        seq_list = []
        for f in table_fields:
            if isinstance(f, models.AutoField):
                seq_list.append({'table': table_name, 'column': f.column})
                break
        return seq_list

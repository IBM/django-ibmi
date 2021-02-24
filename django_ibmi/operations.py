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
# | Authors: Ambrish Bhargava, Tarun Pasrija, Rahul Priyadarshi,             |
# | Hemlata Bhatt, Vyshakh A                                                 |
# +--------------------------------------------------------------------------+
import datetime, pytz
from django.db import utils
from django.utils.timezone import is_aware, utc
from django.db.backends.base.operations import BaseDatabaseOperations
from django.conf import settings


class DatabaseOperations (BaseDatabaseOperations):

    compiler_module = "django_ibmi.compiler"

    def cache_key_culling_sql(self):
        return '''SELECT cache_key FROM (SELECT cache_key, ( ROW_NUMBER() OVER() ) AS ROWNUM FROM %s ORDER BY cache_key)
            WHERE ROWNUM = %%s + 1'''

    def check_aggregate_support(self, aggregate):
        # In DB2 data type of the result is the same as the data type of the
        # argument values for AVG aggregation
        # But Django aspect in Float regardless of data types of argument value
        # http://publib.boulder.ibm.com/infocenter/db2luw/v9r7/index.jsp?topic=
        # /com.ibm.db2.luw.apdv.cli.doc/doc/c0007645.html
        if aggregate.sql_function == 'AVG':
            aggregate.sql_template = '%(function)s(DOUBLE(%(field)s))'
        # In DB2 equivalent sql function of STDDEV_POP is STDDEV
        elif aggregate.sql_function == 'STDDEV_POP':
            aggregate.sql_function = 'STDDEV'
        # In DB2 equivalent sql function of VAR_SAMP is VARIENCE
        elif aggregate.sql_function == 'VAR_POP':
            aggregate.sql_function = 'VARIANCE'
        # DB2 doesn't have sample standard deviation function
        elif aggregate.sql_function == 'STDDEV_SAMP':
            raise NotImplementedError(
                "sample standard deviation function not supported")
        # DB2 doesn't have sample variance function
        elif aggregate.sql_function == 'VAR_SAMP':
            raise NotImplementedError("sample variance function not supported")

    def get_db_converters(self, expression):
        converters = super().get_db_converters(expression)

        field_type = expression.output_field.get_internal_type()
        if field_type in ('BinaryField',):
            converters.append(self.convert_binaryfield_value)
        """Get a list of functions needed to convert field data.

        Some field types on some backends do not provide data in the correct
        format, this is the hook for coverter functions.
        """
        return converters

    def combine_expression(self, operator, sub_expressions):
        if operator == '%%':
            return 'MOD(%s, %s)' % (sub_expressions[0], sub_expressions[1])
        elif operator == '&':
            return 'BITAND(%s, %s)' % (sub_expressions[0], sub_expressions[1])
        elif operator == '|':
            return 'BITOR(%s, %s)' % (sub_expressions[0], sub_expressions[1])
        elif operator == '^':
            return 'POWER(%s, %s)' % (sub_expressions[0], sub_expressions[1])
        elif operator == '-':
            strr = str(sub_expressions[1])
            sub_expressions[1] = strr.replace('+', '-')
            return super().combine_expression(operator, sub_expressions)
        else:
            strr = str(sub_expressions[1])
            sub_expressions[1] = strr.replace('+', '-')
            return super().combine_expression(operator, sub_expressions)

    @staticmethod
    def convert_binaryfield_value(value, expression, connections,context):
        return value

    @staticmethod
    def format_for_duration_arithmetic(sql):
        return ' %s MICROSECONDS' % sql

    # Function to extract day, month or year from the date. Reference:
    # http://publib.boulder.ibm.com/infocenter/db2luw/v9r5/topic/com.ibm.db2
    # .luw.sql.ref.doc/doc/r0023457.html
    def date_extract_sql(self, lookup_type, field_name):
        if lookup_type.upper() == 'WEEK_DAY':
            return " DAYOFWEEK(%s) " % field_name
        else:
            return " %s(%s) " % (lookup_type.upper(), field_name)

    @staticmethod
    def _get_utcoffset(tzname):
        tz = pytz.timezone(tzname)
        td = tz.utcoffset(datetime.datetime(2012, 1, 1))
        if td.days is -1:
            minute = (td.seconds % (60 * 60)) / 60 - 60
            if minute:
                hr = td.seconds / (60 * 60) - 23
            else:
                hr = td.seconds / (60 * 60) - 24
        else:
            hr = td.seconds / (60 * 60)
            minute = (td.seconds % (60 * 60)) / 60
        return hr, minute

    # Function to extract time zone-aware day, month or day of week from
    # timestamps
    def datetime_extract_sql(self, lookup_type, field_name, tzname):
        if settings.USE_TZ:
            hr, minute = self._get_utcoffset(tzname)
            if hr < 0:
                field_name = "%s - %s HOURS - %s MINUTES" % (
                    field_name, -hr, -minute)
            else:
                field_name = "%s + %s HOURS + %s MINUTES" % (
                    field_name, hr, minute)

        if lookup_type.upper() == 'WEEK_DAY':
            return " DAYOFWEEK(%s) " % field_name, []
        else:
            return " %s(%s) " % (lookup_type.upper(), field_name), []

    # Truncating the date value on the basic of lookup type. e.g If input is
    # 2008-12-04 and month then output will be 2008-12-01 00:00:00 Reference:
    # http://www.ibm.com/developerworks/data/library/samples/db2/0205udfs
    # /index.html
    def date_trunc_sql(self, lookup_type, field_name):
        sql = "TIMESTAMP(DATE(SUBSTR(CHAR(%s), 1, %d) || '%s'), TIME('00:00:00'))"
        if lookup_type.upper() == 'DAY':
            sql = sql % (field_name, 10, '')
        elif lookup_type.upper() == 'MONTH':
            sql = sql % (field_name, 7, '-01')
        elif lookup_type.upper() == 'YEAR':
            sql = sql % (field_name, 4, '-01-01')
        return sql

    # Truncating the time zone-aware timestamps value on the basic of lookup
    # type
    def datetime_trunc_sql(self, lookup_type, field_name, tzname):
        sql = "TIMESTAMP(SUBSTR(CHAR(%s), 1, %d) || '%s')"
        if settings.USE_TZ:
            hr, minute = self._get_utcoffset(tzname)
            if hr < 0:
                field_name = "%s - %s HOURS - %s MINUTES" % (
                    field_name, -hr, -minute)
            else:
                field_name = "%s + %s HOURS + %s MINUTES" % (
                    field_name, hr, minute)
        if lookup_type.upper() == 'SECOND':
            sql = sql % (field_name, 19, '.000000')
        if lookup_type.upper() == 'MINUTE':
            sql = sql % (field_name, 16, '.00.000000')
        elif lookup_type.upper() == 'HOUR':
            sql = sql % (field_name, 13, '.00.00.000000')
        elif lookup_type.upper() == 'DAY':
            sql = sql % (field_name, 10, '-00.00.00.000000')
        elif lookup_type.upper() == 'MONTH':
            sql = sql % (field_name, 7, '-01-00.00.00.000000')
        elif lookup_type.upper() == 'YEAR':
            sql = sql % (field_name, 4, '-01-01-00.00.00.000000')
        return sql, []

    def date_interval_sql(self, timedelta):
        return " %d days + %d seconds + %d microseconds" % (
            timedelta.days, timedelta.seconds, timedelta.microseconds), []

    # As casting is not required, so nothing is required to do in this function.
    def datetime_cast_date_sql(self, field_name, tzname):
        return "%s"

    def deferrable_sql(self):
        return "ON DELETE NO ACTION NOT ENFORCED"

    def datetime_cast_time_sql(self, field_name, tzname):
        return "%s"

    def time_trunc_sql(self, lookup_type, field_name):
        sql = "TIMESTAMP(SUBSTR(CHAR(%s), 1, %d) || '%s')"
        if lookup_type.upper() == 'SECOND':
            sql = sql % (field_name, 19, '.000000')
        if lookup_type.upper() == 'MINUTE':
            sql = sql % (field_name, 16, '.00.000000')
        elif lookup_type.upper() == 'HOUR':
            sql = sql % (field_name, 13, '.00.00.000000')
        return sql, []

    # Dropping auto generated property of the identity column.
    def drop_sequence_sql(self, table):
        return "ALTER TABLE %s ALTER COLUMN ID DROP IDENTITY" % \
               (self.quote_name(table))

    # This function casts the field and returns it for use in the where clause
    def field_cast_sql(self, db_type, internal_type=None):
        if db_type == 'CLOB':
            return "VARCHAR(%s, 4096)"
        else:
            return " %s"

    # Function to return value of auto-generated field of last executed
    # insert query.
    def last_insert_id(self, cursor, table_name, pk_name):
        operation = 'SELECT IDENTITY_VAL_LOCAL() FROM SYSIBM.SYSDUMMY1'
        result = cursor.execute(operation)
        return result.fetchone()[0]

    # In case of WHERE clause, if the search is required to be case
    # insensitive then converting left hand side field to upper.
    def lookup_cast(self, lookup_type, internal_type=None):
        if lookup_type in ('iexact', 'icontains', 'istartswith', 'iendswith'):
            return "UPPER(%s)"
        return "%s"

    # As DB2 v91 specifications,
    # Maximum length of a table name and Maximum length of a column name is 128
    # http://publib.boulder.ibm.com/infocenter/db2e/v9r1/index.jsp?topic=/
    # com.ibm.db2e.doc/db2elimits.html
    def max_name_length(self):
        return 128

    def no_limit_value(self):
        return None

    def quote_name(self, name):
        name = name.upper()
        if name.startswith("\"") & name.endswith("\""):
            return name

        if name.startswith("\""):
            return "%s\"" % name

        if name.endswith("\""):
            return "\"%s" % name

        return "\"%s\"" % name

    # SQL to return RANDOM number.
    # Reference: http://publib.boulder.ibm.com/infocenter/db2luw/v8/topic/com.
    # ibm.db2.udb.doc/admin/r0000840.htm
    def random_function_sql(self):
        return "SYSFUN.RAND()"

    def regex_lookup(self, lookup_type):
        if lookup_type == 'regex':
            return '''xmlcast( xmlquery('fn:matches(xs:string($c), "%%s")'
            passing %s as "c") as varchar(5)) = 'true' db2regexExtraField(%s)
            '''
        else:
            return '''xmlcast( xmlquery('fn:matches(xs:string($c), "%%s",
            "i")' passing %s as "c") as varchar(5)) = 'true'
            db2regexExtraField(%s) '''

    # As save-point is supported by DB2, following function will return SQL
    # to create savepoint.
    def savepoint_create_sql(self, sid):
        return "SAVEPOINT %s ON ROLLBACK RETAIN CURSORS" % sid

    # Function to commit savepoint.
    def savepoint_commit_sql(self, sid):
        return "RELEASE TO SAVEPOINT %s" % sid

    # Function to rollback savepoint.
    def savepoint_rollback_sql(self, sid):
        return "ROLLBACK TO SAVEPOINT %s" % sid

    # Deleting all the rows from the list of tables provided and resetting
    # all the sequences.
    def sql_flush(self, style, tables, sequences, allow_cascade=False):
        curr_schema = self.connection.get_current_schema().upper()
        sqls = []
        fk_tab = 'TABNAME'
        fk_tabschema = 'TABSCHEMA'
        fk_const = 'CONSTNAME'
        fk_systab = 'SYSCAT.TABCONST'
        type_check_string = "type = 'F' and"
        sqls.append('''CREATE PROCEDURE FKEY_ALT_CONST(django_tabname VARCHAR(128), curr_schema VARCHAR(128))
                    LANGUAGE SQL
                    P1: BEGIN
                        DECLARE fktable varchar(128);
                        DECLARE fkconst varchar(128);
                        DECLARE row_count integer;
                        DECLARE alter_fkey_sql varchar(350);
                        DECLARE cur1 CURSOR for SELECT %(fk_tab)s, %(fk_const)s FROM %(fk_systab)s WHERE %(type_check_string)s
                        %(fk_tabschema)s = curr_schema and ENFORCED = 'N';
                        DECLARE cur2 CURSOR for SELECT %(fk_tab)s, %(fk_const)s FROM %(fk_systab)s WHERE %(type_check_string)s
                        %(fk_tab)s = django_tabname and %(fk_tabschema)s = curr_schema and ENFORCED = 'Y';
                        IF ( django_tabname = '' ) THEN
                            SET row_count = 0;
                            SELECT count( * ) INTO row_count FROM %(fk_systab)s WHERE %(type_check_string)s %(fk_tabschema)s =
                            curr_schema and ENFORCED = 'N';
                            IF ( row_count > 0 ) THEN
                                OPEN cur1;
                                WHILE( row_count > 0 ) DO
                                    FETCH cur1 INTO fktable, fkconst;
                                    IF ( LOCATE( ' ', fktable ) > 0 ) THEN
                                        SET alter_fkey_sql = 'ALTER TABLE ' || '\"' || fktable || '\"' ||' ALTER FOREIGN KEY ';
                                    ELSE
                                        SET alter_fkey_sql = 'ALTER TABLE ' || fktable || ' ALTER FOREIGN KEY ';
                                    END IF;
                                    IF ( LOCATE( ' ', fkconst ) > 0) THEN
                                        SET alter_fkey_sql = alter_fkey_sql || '\"' || fkconst || '\"' || ' ENFORCED';
                                    ELSE
                                        SET alter_fkey_sql = alter_fkey_sql || fkconst || ' ENFORCED';
                                    END IF;
                                    execute immediate alter_fkey_sql;
                                    SET row_count = row_count - 1;
                                END WHILE;
                                CLOSE cur1;
                            END IF;
                        ELSE
                            SET row_count = 0;
                            SELECT count( * ) INTO row_count FROM %(fk_systab)s WHERE %(type_check_string)s %(fk_tab)s =
                            django_tabname and %(fk_tabschema)s = curr_schema and ENFORCED = 'Y';
                            IF ( row_count > 0 ) THEN
                                OPEN cur2;
                                WHILE( row_count > 0 ) DO
                                    FETCH cur2 INTO fktable, fkconst;
                                    IF ( LOCATE( ' ', fktable ) > 0 ) THEN
                                        SET alter_fkey_sql = 'ALTER TABLE ' || '\"' || fktable || '\"' ||' ALTER FOREIGN KEY ';
                                    ELSE
                                        SET alter_fkey_sql = 'ALTER TABLE ' || fktable || ' ALTER FOREIGN KEY ';
                                    END IF;
                                    IF ( LOCATE( ' ', fkconst ) > 0) THEN
                                        SET alter_fkey_sql = alter_fkey_sql || '\"' || fkconst || '\"' || ' NOT ENFORCED';
                                    ELSE
                                        SET alter_fkey_sql = alter_fkey_sql || fkconst || ' NOT ENFORCED';
                                    END IF;
                                    execute immediate alter_fkey_sql;
                                    SET row_count = row_count - 1;
                                END WHILE;
                                CLOSE cur2;
                            END IF;
                        END IF;
                    END P1''' % {'fk_tab': fk_tab, 'fk_tabschema': fk_tabschema,
                                 'fk_const': fk_const, 'fk_systab': fk_systab,
                                 'type_check_string': type_check_string})

        for table in tables:
            sqls.append("CALL FKEY_ALT_CONST( '%s', '%s' );" %
                        (table.upper(), curr_schema))

        for table in tables:
            sqls.append(style.SQL_KEYWORD("DELETE") + " " +
                        style.SQL_KEYWORD("FROM") + " " +
                        style.SQL_TABLE("%s" % self.quote_name(table)))

        sqls.append("CALL FKEY_ALT_CONST( '' , '%s' );" % (curr_schema,))
        sqls.append("DROP PROCEDURE FKEY_ALT_CONST;")

        for sequence in sequences:
            if sequence['column'] is not None:
                sqls.append(style.SQL_KEYWORD("ALTER TABLE") + " " +
                            style.SQL_TABLE(
                                "%s" % self.quote_name(sequence['table'])) +
                            " " +
                            style.SQL_KEYWORD("ALTER COLUMN") + " %s "
                            % self.quote_name(sequence['column']) +
                            style.SQL_KEYWORD("RESTART WITH 1"))
        return sqls

    # Table many contains rows when this is get called, hence resetting sequence
    # to a large value (10000).
    def sequence_reset_sql(self, style, model_list):
        from django.db import models
        cursor = self.connection.cursor()
        sqls = []
        for model in model_list:
            table = model._meta.db_table
            for field in model._meta.local_fields:
                if isinstance(field, models.AutoField):
                    max_sql = "SELECT MAX(%s) FROM %s" % (
                        self.quote_name(field.column), self.quote_name(table))
                    cursor.execute(max_sql)
                    max_id = [row[0] for row in cursor.fetchall()]
                    if max_id[0] is None:
                        max_id[0] = 0
                    sqls.append(style.SQL_KEYWORD("ALTER TABLE") + " " +
                                style.SQL_TABLE("%s" % self.quote_name(table)) +
                                " " +
                                style.SQL_KEYWORD("ALTER COLUMN") + " %s "
                                % self.quote_name(field.column) +
                                style.SQL_KEYWORD("RESTART WITH %s" % (max_id[0] + 1)))
                    break

            for field in model._meta.many_to_many:
                m2m_table = field.m2m_db_table()
                if field.remote_field is not None and hasattr(
                        field.remote_field, 'through'):
                    flag = field.remote_field.through
                else:
                    flag = False
                if not flag:
                    max_sql = "SELECT MAX(%s) FROM %s" % (
                        self.quote_name('ID'), self.quote_name(table))
                    cursor.execute(max_sql)
                    max_id = [row[0] for row in cursor.fetchall()]
                    if max_id[0] is None:
                        max_id[0] = 0
                    sqls.append(style.SQL_KEYWORD("ALTER TABLE") + " " +
                                style.SQL_TABLE(
                                    "%s" % self.quote_name(m2m_table)) +
                                " " +
                                style.SQL_KEYWORD("ALTER COLUMN") + " %s "
                                % self.quote_name('ID') +
                                style.SQL_KEYWORD("RESTART WITH %s" %
                                                  (max_id[0] + 1)))
        if cursor:
            cursor.close()

        return sqls

    # Returns sqls to reset the passed sequences
    def sequence_reset_by_name_sql(self, style, sequences):
        sqls = []
        for seq in sequences:
            sqls.append(style.SQL_KEYWORD("ALTER TABLE") + " " +
                        style.SQL_TABLE("%s" % self.quote_name(
                            seq.get('table'))) +
                        " " + style.SQL_KEYWORD("ALTER COLUMN") + " %s " %
                        self.quote_name(seq.get('column')) +
                        style.SQL_KEYWORD("RESTART WITH %s" % 1))
        return sqls

    def tablespace_sql(self, tablespace, inline=False):
        # inline is used for column indexes defined in-line with column
        # definition, like: CREATE TABLE "TABLE1" ("ID_OTHER" VARCHAR(20) NOT
        # NULL UNIQUE) IN "TABLESPACE1"; couldn't find support for this in
        # create table (http://publib.boulder.ibm.com/infocenter/db2luw/v9
        # /topic/com.ibm.db2.udb.admin.doc/doc/r0000927.htm)
        if inline:
            sql = ""
        else:
            sql = "IN %s" % self.quote_name(tablespace)
        return sql

    def adapt_datetimefield_value(self, value):
        if value is None:
            return None

        if is_aware(value):
            if settings.USE_TZ:
                value = value.astimezone(utc).replace(tzinfo=None)
            else:
                raise ValueError("Timezone aware datetime not supported")
        return str(value)

    def adapt_timefield_value(self, value):
        if value is None:
            return None

        if is_aware(value):
            raise ValueError("Timezone aware time not supported")
        else:
            return str(value)

    def year_lookup_bounds_for_date_field(self, value):
        lower_bound = datetime.date(int(value), 1, 1)
        upper_bound = datetime.date(int(value), 12, 31)
        return [lower_bound, upper_bound]

    def bulk_insert_sql(self, fields, num_values):
        values_sql = "( %s )" % (", ".join(["%s"] * len(fields)))
        if isinstance(num_values, int):
            bulk_values_sql = "VALUES " + ", ".join([values_sql] * num_values)
        else:
            bulk_values_sql = "VALUES " + ", ".join([values_sql] * len(num_values))
        return bulk_values_sql

    def for_update_sql(self, nowait=False, skip_locked=False, of=()):
        # DB2 doesn't support nowait select for update
        if nowait:
            raise utils.DatabaseError(
                "Nowait Select for update not supported ")
        else:
            return 'WITH RS USE AND KEEP UPDATE LOCKS'

    def distinct_sql(self, fields, params):
        if fields:
            raise ValueError("distinct_on_fields not supported")
        else:
            return 'DISTINCT'

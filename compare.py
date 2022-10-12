from random import random
import psycopg
import os
from dotenv import load_dotenv

def write_final(text):
    with open('output.txt', 'w') as f:
        f.write(str(text))
    exit()

load_dotenv()

# 'information_schema.columns' columns to compare
schema_columns = ['data_type', 'column_default', 'is_nullable', 'character_maximum_length', 'numeric_precision']

dbconn1 = psycopg.connect("host={} port={} dbname={} user={}".format(os.getenv('HOST'), os.getenv('PORT'), os.getenv('DBNAME1'), os.getenv('USER')))
cur1 = dbconn1.cursor()

dbconn2 = psycopg.connect("host={} port={} dbname={} user={}".format(os.getenv('HOST'), os.getenv('PORT'), os.getenv('DBNAME2'), os.getenv('USER')))
cur2 = dbconn2.cursor()

# compare table names
cur1.execute('SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %s', [os.getenv('SCHEMA1')])
tables1 = cur1.fetchall()
cur2.execute('SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %s', [os.getenv('SCHEMA2')])
tables2 = cur2.fetchall()
diff = set(tables1) ^ set(tables2)
if diff:
    diff_tables = [item[0] for item in diff]
    text = "diff tables:\n\t{}".format(',\n\t'.join(diff_tables))
    write_final(text)

# compare table schemas
diff_output = []
schema = []
for table in tables1:
    q = 'SELECT column_name, {} FROM information_schema.columns WHERE table_name = %s AND table_schema = %s ORDER BY column_name ASC'.format(', '.join(schema_columns))
    cur1.execute(q, [table[0], os.getenv('SCHEMA1')])
    schema1 = cur1.fetchall()
    schema.append(schema1)
    cur2.execute(q, [table[0], os.getenv('SCHEMA2')])
    schema2 = cur2.fetchall()

    if len(schema1)==0 or len(schema2)==0:
        errm = 'could not get columns on table `{}`, database {}'.format(table[0], '1 and 2' if len(schema1)==0 and len(schema2)==0 else '1' if len(schema1)==0 else '2')
        # diff_output.append(errm)
        print(errm)
        continue

    # compare column count and names
    columns1 = [item[0] for item in schema1]
    columns2 = [item[0] for item in schema2]
    diff = set(columns1) ^ set(columns2)
    if diff:
        diff_columns = [item[0] for item in diff]
        diff_output.append('diff table column names on `{}`:\n\t{}'.format(table[0], ',\n\t'.join(diff_columns)))
        continue

    table_diffs = []
    for i in range(len(schema1)):
        col_diffs = []
        if schema1[i][0] == 'gid':
            continue
        for j in range(1, len(schema1[i])):
            if schema1[i][j] != schema2[i][j]:
                col_diffs.append('\t\tdiff {}:\n\t\t\t1:\t{}\n\t\t\t2:\t{}'.format(schema_columns[j-1], str(schema1[i][j]), str(schema2[i][j])))
        if col_diffs:
            table_diffs.append('\tdiff column schema on `{}`\n{}'.format(schema1[i][0], '\n'.join(col_diffs)))
    if table_diffs:
        diff_output.append('diff table schema on `{}`\n{}'.format(table[0], '\n'.join(table_diffs)))
if diff_output:
    write_final('\n'.join(diff_output))

#compare data
# for i in range(len(tables1)):
#     match_field = None
#     for field in schema[i]:
#         if 'hash' in field:
#             match_field = field
#     if not match_field:
#         for field in schema[i]:
#             if 'geom' in field:
#                 match_field = "MD5({})".format(field)
#     if not match_field:
#         continue

#     schema[i].remove('gid')
#     select_str = ', '.join(schema[i])

#     cur1.execute('SELECT {}')

write_final('equal')

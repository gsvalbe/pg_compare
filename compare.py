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
tables1 = [item[0] for item in cur1.fetchall()]
cur2.execute('SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %s', [os.getenv('SCHEMA2')])
tables2 = [item[0] for item in cur2.fetchall()]
diff_output = list(set(tables1) ^ set(tables2))
if diff_output:
    text = "diff tables:\n\t{}".format(',\n\t'.join(diff_output))
    write_final(text)

# compare table schemas
column_names = []
for table in tables1:
    q = 'SELECT column_name, {} FROM information_schema.columns WHERE table_name = %s AND table_schema = %s ORDER BY column_name ASC'.format(', '.join(schema_columns))
    cur1.execute(q, [table, os.getenv('SCHEMA1')])
    schema1 = cur1.fetchall()
    cur2.execute(q, [table, os.getenv('SCHEMA2')])
    schema2 = cur2.fetchall()

    if len(schema1)==0 or len(schema2)==0:
        errm = 'could not get columns on table `{}`, database {}'.format(table, '1 and 2' if len(schema1)==0 and len(schema2)==0 else '1' if len(schema1)==0 else '2')
        # diff_output.append(errm)
        print(errm)
        continue

    # compare column count and names
    columns1 = [item[0] for item in schema1]
    columns2 = [item[0] for item in schema2]
    column_names.append(columns1)
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

# compare data
for i in range(2): #len(tables1)
    order_field = None
    for field in column_names[i]:
        if 'hash' in field:
            order_field = field
    if not order_field:
        for field in column_names[i]:
            if 'geom' in field:
                order_field = 'MD5(CAST({} AS TEXT))'.format(field)
    if not order_field:
        continue

    if 'gid' in column_names[i]:
        column_names[i].remove('gid')
    select_str = order_field+', '+', '.join(column_names[i])

    finished = False
    offset1 = 0
    offset2 = 0
    diffs = []
    while offset1 < 100000:
        cur1.execute('SELECT {} FROM {} ORDER BY {} ASC LIMIT 1000 OFFSET {}'.format(select_str, tables1[i], order_field, offset1))
        data1 = cur1.fetchall()
        cur2.execute('SELECT {} FROM {} ORDER BY {} ASC LIMIT 1000 OFFSET {}'.format(select_str, tables1[i], order_field, offset2))
        data2 = cur2.fetchall()
        if len(data1) == 0 and len(data2) == 0:
            break
        i1=0
        i2=0
        offset1+=1000
        offset2+=1000
        while i1<len(data1) or i2<len(data2):
            if len(data2) <= i2:
                diffs.append("database 2 table `{}` missing item from database 1 `WHERE {} = '{}'`".format(tables1[i], order_field, str(data1[i1][0])))
                i2+=1
                offset1-=1
                continue
            if len(data1) <= i1:
                diffs.append("database 1 table `{}` missing item from database 2 `WHERE {} = '{}'`".format(tables1[i], order_field, str(data2[i2][0])))
                i1+=1
                offset2-=1
                continue
            if data1[i1][0] > data2[i2][0]:
                diffs.append("database 2 table `{}` missing item from database 1 `WHERE {} = '{}'`".format(tables1[i], order_field, str(data1[i1][0])))
                i2+=1
                offset1-=1
                continue
            if data1[i1][0] < data2[i2][0]:
                diffs.append("database 1 table `{}` missing item from database 2 `WHERE {} = '{}'`".format(tables1[i], order_field, str(data2[i2][0])))
                i1+=1
                offset2-=1
                continue
            i1+=1
            i2+=1
    if diffs:
        diff_output.append('diff table data on `{}`:\n\t{}'.format(tables1[i], '\n\t'.join(diffs)))
if diff_output:
    write_final('\n'.join(diff_output))
        

write_final('equal')

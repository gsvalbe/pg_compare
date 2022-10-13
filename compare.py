import psycopg
import os
from dotenv import load_dotenv

load_dotenv()

output = open('output.txt', 'w', encoding='utf-8')

# 'information_schema.columns' columns to compare
schema_columns = ['data_type', 'is_nullable', 'numeric_precision']
skip_tables = ['django_migrations', 'kijs_adrese_lv_old_old_vers', 'kijs_adrese_lv_old_OLD_VERS', 'kijs_poi','kijs_iela','kijs_iela_detail','kijs_ielu_posmi']
skip_columns = ['gid', 'id']

dbconn1 = psycopg.connect("host={} port={} dbname={} user={}".format(os.getenv('HOST'), os.getenv('PORT'), os.getenv('DBNAME1'), os.getenv('USER')))
cur1 = dbconn1.cursor()

dbconn2 = psycopg.connect("host={} port={} dbname={} user={}".format(os.getenv('HOST'), os.getenv('PORT'), os.getenv('DBNAME2'), os.getenv('USER')))
cur2 = dbconn2.cursor()

# compare table names
cur1.execute('SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %s', [os.getenv('SCHEMA1')])
tables1 = [item[0] for item in cur1.fetchall()]
cur2.execute('SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %s', [os.getenv('SCHEMA2')])
tables2 = [item[0] for item in cur2.fetchall()]
tables1 = set(tables1) - set(skip_tables)
tables2 = set(tables2) - set(skip_tables)
diff_output = list(tables1 ^ tables2)
if diff_output:
    text = "diff tables:\n\t{}".format(',\n\t'.join(diff_output))
    output.write(text)
    output.close()
    exit()
print('table names equal')

# compare table schemas
column_names = []
stop = False
for table in tables1:
    q = 'SELECT column_name, {} FROM information_schema.columns WHERE table_name = %s AND table_schema = %s ORDER BY column_name ASC'.format(', '.join(schema_columns))
    cur1.execute(q, [table, os.getenv('SCHEMA1')])
    schema1 = cur1.fetchall()
    cur2.execute(q, [table, os.getenv('SCHEMA2')])
    schema2 = cur2.fetchall()

    if len(schema1)==0 or len(schema2)==0:
        errm = "could not get columns on table '{}', database {}".format(table, '1 and 2' if len(schema1)==0 and len(schema2)==0 else '1' if len(schema1)==0 else '2')
        # diff_output.append(errm)
        print(errm)
        continue

    for sch in [schema1, schema2]:
        c=0
        while c < len(sch):
            if sch[c][0] in skip_columns:
                del sch[c]
            else:
                c+=1
    # compare column count and names
    columns1 = [item[0] for item in schema1]
    columns2 = [item[0] for item in schema2]
    column_names.append(columns1)
    diff = set(columns1) ^ set(columns2)
    if diff:
        output.write("diff table column names on '{}':\n\t{}\n".format(table, ',\n\t'.join(diff)))
        stop = True
        continue

    for i in range(len(schema1)):
        col_diffs = []
        for j in range(1, len(schema1[i])):
            if schema1[i][j] != schema2[i][j]:
                col_diffs.append('{}: \n\t\t1) {} \n\t\t2) {}'.format(schema_columns[j-1], str(schema1[i][j]), str(schema2[i][j])))
        if col_diffs:
            output.write("'{}'->'{}':\n\t{}\n".format(table, schema1[i][0], '\n\t\t'.join(col_diffs)))
            stop = True
# if stop:
    # output.close()
    # exit()
print('table schemas equal')


# compare data
for i in range(len(tables1)): #len(tables1)
    table = list(tables1)[i]
    print("table '{}'".format(table))
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

    select_str = order_field+', '+', '.join(column_names[i])

    finished = False
    offset1 = 0
    offset2 = 0
    diffs = []
    while offset1 < 500000 and offset2 < 500000:
        cur1.execute('SELECT {} FROM {} ORDER BY {} ASC LIMIT 1000 OFFSET {}'.format(select_str, table, order_field, offset1))
        data1 = cur1.fetchall()
        cur2.execute('SELECT {} FROM {} ORDER BY {} ASC LIMIT 1000 OFFSET {}'.format(select_str, table, order_field, offset2))
        data2 = cur2.fetchall()
        if len(data1) == 0 and len(data2) == 0:
            break
        i1=0
        i2=0
        offset1+=1000
        offset2+=1000
        while (i1<len(data1) or i2<len(data2)) and i1<10000 and i2<10000:
            if len(data2) <= i2:
                output.write("db 2 table '{}' missing from db 1 `WHERE {} = '{}'`\n".format(table, order_field, str(data1[i1][0])))
                i2+=1
                continue
            if len(data1) <= i1:
                output.write("db 1 table '{}' missing from db 2 `WHERE {} = '{}'`\n".format(table, order_field, str(data2[i2][0])))
                i1+=1
                continue
            if data1[i1][0] > data2[i2][0]:
                output.write("db 2 table '{}' missing from db 1 `WHERE {} = '{}'`\n".format(table, order_field, str(data1[i1][0])))
                i2+=1
                continue
            if data1[i1][0] < data2[i2][0]:
                output.write("db 1 table '{}' missing from db 2 `WHERE {} = '{}'`\n".format(table, order_field, str(data2[i2][0]))) 
                i1+=1
                continue
            col_diffs = []
            for col in range(1, len(column_names[i])+1):
                if data1[i1][col] != data2[i2][col]:
                    col_diffs.append("'{}':  1)'{}', 2)'{}'".format(column_names[i][col-1], data1[i1][col], data2[i2][col]))
            if col_diffs:
                output.write("table '{}' row data: \n\t\t{}\n".format(table, '\n\t\t'.join(col_diffs)))
            i1+=1
            i2+=1

output.close()

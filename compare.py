import psycopg
import os
from dotenv import load_dotenv

load_dotenv()

output = open('output.txt', 'w', encoding='utf-8')

# 'information_schema.columns' columns to compare
schema_columns = ['data_type', 'is_nullable', 'numeric_precision']
skip_tables = ['django_migrations', 'spatial_ref_sys', 'kijs_adrese_lv_old_old_vers', 'kijs_adrese_lv_old_OLD_VERS', 'kijs_poi', 'kijs_iela', 'kijs_iela_detail', 'kijs_ielu_posmi']
skip_columns = ['gid', 'id']
table_detailed_max = 50

dbconn1 = psycopg.connect("host={} port={} dbname={} user={}".format(os.getenv('HOST'), os.getenv('PORT'), os.getenv('DBNAME1'), os.getenv('USER')))
cur1 = dbconn1.cursor()

dbconn2 = psycopg.connect("host={} port={} dbname={} user={}".format(os.getenv('HOST'), os.getenv('PORT'), os.getenv('DBNAME2'), os.getenv('USER')))
cur2 = dbconn2.cursor()


def compare_table_names():
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
        return False, list(tables1)
    print('table names equal')
    return True, list(tables1)


def compare_table_schemas(table):
    output.write("\n------------------- '{}' -------------------\n".format(table))
    q = 'SELECT column_name, {} FROM information_schema.columns WHERE table_name = %s AND table_schema = %s ORDER BY column_name ASC'.format(', '.join(schema_columns))
    cur1.execute(q, [table, os.getenv('SCHEMA1')])
    schema1 = cur1.fetchall()
    cur2.execute(q, [table, os.getenv('SCHEMA2')])
    schema2 = cur2.fetchall()

    if len(schema1)==0 or len(schema2)==0:
        errm = "could not get columns on table '{}', database {}".format(table, '1 and 2' if len(schema1)==0 and len(schema2)==0 else '1' if len(schema1)==0 else '2')
        print(errm)
        return False, []

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
    column_names = columns1
    diff = set(columns1) ^ set(columns2)
    if diff:
        output.write("diff table column names on '{}':\n\t{}\n".format(table, ',\n\t'.join(diff)))
        return False, column_names

    res = True
    for i in range(len(schema1)):
        col_diffs = []
        for j in range(1, len(schema1[i])):
            if schema1[i][j] != schema2[i][j]:
                col_diffs.append('{}: \n\t\t1) {} \n\t\t2) {}'.format(schema_columns[j-1], str(schema1[i][j]), str(schema2[i][j])))
        if col_diffs:
            output.write("'{}'->'{}':\n\t{}\n".format(table, schema1[i][0], '\n\t\t'.join(col_diffs)))
            res = False
    return res, column_names


def compare_data(table, column_names):
    print("table '{}'".format(table))

    cur1.execute('SELECT COUNT(*) FROM {}'.format(table))
    cur2.execute('SELECT COUNT(*) FROM {}'.format(table))
    c1 = cur1.fetchone()[0]
    c2 = cur2.fetchone()[0]
    if c1==0 and c2 == 0:
        output.write("tables are empty\n")
        return
    if abs(c1 - c2) > 1000:
        output.write("very large count diff on '{}':\n\t1) {}, 2) {}\n".format(table, str(c1), str(c2)))
        print("very large count diff on '{}'".format(table))
        return

    o_field = None
    order_field = None
    md5 = False
    ofc1=0
    ofc2=0
    for field in column_names:
        if 'hash' in field:
            o_field = field
    if not o_field:
        for field in column_names:
            if 'geo' in field:
                o_field = field
                md5 = True
    
    if o_field:
        order_field = o_field if md5 else 'MD5({}::TEXT)'.format(o_field)
        cur1.execute('SELECT COUNT(DISTINCT({})) FROM {}'.format('MD5({}::TEXT)'.format(o_field) if md5 else o_field, table))
        cur2.execute('SELECT COUNT(DISTINCT({})) FROM {}'.format('MD5({}::TEXT)'.format(o_field) if md5 else o_field, table))
        ofc1 = cur1.fetchone()[0]
        ofc2 = cur2.fetchone()[0]

    if ofc1 < c1 or ofc2 < c2:
        for field in column_names:
            if 'name' in field or 'code' in field or 'index' in field:
                o_field += ', ' + field
                order_field = 'MD5(ROW({})::TEXT)'.format(o_field)
        if not order_field:
            output.write("no order field found on '{}'\n".format(table))
            print("no order field found on '{}'".format(table))
            return


    select_str = order_field+', '+', '.join(column_names)
    offset1 = 0
    offset2 = 0

    missing1 = 0
    missing2 = 0
    checked_rows = 0
    diff_col = {}
    for i in range(len(column_names)):
        diff_col[i] = 0
    print_detailed = True

    while offset1 < 500000 and offset2 < 500000:
        cur1.execute('SELECT {} FROM {} ORDER BY {} ASC LIMIT 1000 OFFSET {}'.format(select_str, table, order_field, offset1))
        data1 = cur1.fetchall()
        cur2.execute('SELECT {} FROM {} ORDER BY {} ASC LIMIT 1000 OFFSET {}'.format(select_str, table, order_field, offset2))
        data2 = cur2.fetchall()
        if len(data1) == 0 and len(data2) == 0:
            break
        i1=0
        i2=0
        while i1<len(data1) and i2<len(data2):
            if print_detailed:
                if missing1 + missing2 + sum(diff_col.values()) > table_detailed_max:
                    output.write('...\n')
                    print_detailed = False
            if data1[i1][0] > data2[i2][0]:
                missing1 += 1
                if print_detailed:
                    output.write("db 1 '{}' missing `WHERE {} = '{}'`\n".format(table, order_field, str(data1[i1][0])))
                i2+=1
                continue
            if data1[i1][0] < data2[i2][0]:
                missing2 += 1
                if print_detailed:
                    output.write("db 2 '{}' missing `WHERE {} = '{}'`\n".format(table, order_field, str(data2[i2][0]))) 
                i1+=1
                continue
            col_diffs = []
            checked_rows += 1
            for col in range(1, len(column_names)+1):
                if data1[i1][col] != data2[i2][col]:
                    diff_col[col-1] += 1
                    if print_detailed:
                        col_diffs.append("'{}':  1)'{}', 2)'{}'".format(column_names[col-1], data1[i1][col], data2[i2][col]))
            if col_diffs:
                output.write("'{}' row data (db1: {}, db2: {}): \n\t\t{}\n".format(table, str(offset1+i1), str(offset2+i2), '\n\t\t'.join(col_diffs)))
            i1+=1
            i2+=1
        offset1+=1000
        offset2+=1000
    
    if missing1:
        output.write("{} rows missing in db 1\n".format(str(missing1)))
    if missing2:
        output.write("{} rows missing in db 2\n".format(str(missing2)))
    if checked_rows == 0:
        return
    for column, diffcount in diff_col.items():
        if diffcount / checked_rows > 0.95:
            output.write("ALL rows different in '{}'->'{}'\n".format(table, column_names[column]))
        elif diffcount > 0:
            output.write("{} rows different in '{}'->'{}'\n".format(str(diffcount), table, column_names[column]))


res, tables = compare_table_names()
if res:
    for table in tables:
        res, cnames = compare_table_schemas(table)
        if res:
            compare_data(table, cnames)

dbconn1.close()
dbconn2.close()
output.close()

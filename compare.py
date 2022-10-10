import psycopg
import os
from dotenv import load_dotenv

def write_final(text):
    with open('output.txt', 'w') as f:
        f.write(str(text))
    exit()

load_dotenv()

dbconn1 = psycopg.connect("host={} port={} dbname={} user={}".format(os.getenv('HOST'), os.getenv('PORT'), os.getenv('DBNAME1'), os.getenv('USER')))
cur1 = dbconn1.cursor()

dbconn2 = psycopg.connect("host={} port={} dbname={} user={}".format(os.getenv('HOST'), os.getenv('PORT'), os.getenv('DBNAME2'), os.getenv('USER')))
cur2 = dbconn2.cursor()

# compare table names
cur1.execute('SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %s', [os.getenv('SCHEMA1')])
tables1 = cur1.fetchall()
cur2.execute('SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %s', [os.getenv('SCHEMA2')])
tables2 = cur2.fetchall()
tables2 = tables1
diff = set(tables1) - set(tables2)
if diff:
    diff_tables = [item[0] for item in diff]
    text = "different tables:\n\t{}".format(',\n\t'.join(diff_tables))
    write_final(text)

# compare table schemas
diff_output = []
for table in tables1:
    cur1.execute('SELECT column_name, data_type, column_default, is_nullable, character_maximum_length, numeric_precision FROM information_schema.columns WHERE table_name = %s AND table_schema = %s', [table[0], os.getenv('SCHEMA1')])
    schema1 = cur1.fetchall()
    cur2.execute('SELECT column_name, data_type, column_default, is_nullable, character_maximum_length, numeric_precision FROM information_schema.columns WHERE table_name = %s AND table_schema = %s', [table[0], os.getenv('SCHEMA2')])
    schema2 = cur2.fetchall()
    diff = set(schema1) - set(schema2)
    if diff:
        diff_schema = [', '.join([str(i) for i in item]) for item in diff]
        text = "different table schema on table '{}':\n\t{}".format(table[0], ',\n\t'.join(diff_schema))
        diff_output.append(text)
if diff_output:
    write_final('\n'.join(diff_output))

write_final('equal')

#!/usr/bin/python

import glob
import os
import boto3
import re
import psycopg2
from datetime import datetime
from subprocess import Popen


#taking input from teamcity parameters
requirement = os.getenv("requirement")
host        = os.getenv("host_url")
database    = os.getenv("database_name")
user        = os.getenv("user_name")
password    = os.getenv("database_passwd")
schema      = os.getenv("schema_name")
table       = os.getenv("table_name")
bucketname  = os.getenv("bucket_name")
pattern     = os.getenv("pattern_name")

os.system("rm pg*")

matched_schema = []

## ct = current time, dt = date + time, dt_dir = date for directory 
ct = datetime.now()
dt = '-'.join((str(ct.year), str(ct.month), str(ct.day), str(ct.hour), str(ct.minute), str(ct.second)))
dt_dir = '-'.join((str(ct.year), str(ct.month), str(ct.day)))


def authenticate(password):

    p = Popen(command, shell=True, env={**os.environ, "PGPASSWORD": password})
    p.communicate('{}\n'.format(password))


## making connection to fetch schemas
connection = psycopg2.connect(database="{0}".format(database), user="{0}".format(user), password="{0}".format(password), host="{0}".format(host), port="5432")

cursor = connection.cursor()
cursor.execute("SELECT schema_name FROM information_schema.schemata;")
output = cursor.fetchall()

## converting list of tupple into list [(a,),(b,),(c,)] convert into list [a,b,c]
all_schema = [''.join(i) for i in output]

if 'only database' in requirement:
    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -Fc -f pg_{1}_{3}.dump'.format(host,database,user,dt)
    authenticate(password)

elif 'all schema' in requirement:
    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -n {1}.* -Fc -f pg_{1}_{3}.dump'.format(host,database,user,dt)
    authenticate(password)

elif 'pattern schema' in requirement:
    pattern_c = '^' + pattern
    for i in range(len(all_schema)):
        schema_name = all_schema[i]
        result = re.match(pattern_c, schema_name)
        if result:
            matched_schema.append(schema_name)
            command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -n {3} -Fc -f pg_{3}_{4}.dump'.format(host,database,user,schema_name,dt)
            authenticate(password)
    print(matched_schema)
elif 'only schema' in requirement:
    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -n {3} -Fc -f pg_{3}_{4}.dump'.format(host,database,user,schema,dt)
    authenticate(password)

elif 'all table' in requirement:
    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -t {3}.* -Fc -f pg_{3}_{4}.dump'.format(host,database,user,schema,dt)
    authenticate(password)

elif 'only table' in requirement:
    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -t {3}.{4} -Fc -f pg_{4}_{5}.dump'.format(host,database,user,schema,table,dt)
    authenticate(password)

else:
    print("requirement is not match")


connection.close()


session = boto3.Session(
       aws_access_key_id=os.getenv("AWS_ACCESS"),
       aws_secret_access_key=os.getenv("AWS_SECRET"),
     )
s3 = session.resource('s3')
dump_files = glob.glob("/root/pg_*.dump")
for filename in dump_files:
    key = "%s/%s" % ('backup-{0}'.format(dt_dir), os.path.basename(filename))
    print("Putting %s as %s" % (filename,key))
    s3.meta.client.upload_file(filename, bucketname, key)

os.system("rm pg*")

#!/usr/bin/python

import glob
import os
import boto3
from subprocess import PIPE,Popen

requirement = os.getenv("requirement")
host        = os.getenv("host_url")
database    = os.getenv("database_name")
user        = os.getenv("user_name")
password    = os.getenv("PWD")
bucketname  = os.getenv("bucket_name")
schema      = os.getenv("schema_name")
table       = os.getenv("table_name")

def runprocess(password):
    p = Popen(command, shell=True, env={**os.environ, "PGPASSWORD": password})
    p.communicate('{}\n'.format(password))
  

if 'only' in requirement and 'database' in requirement:
    command = 'pg_dump -h {0} -U {2} -p 5432 -d {1} -Fc -f pg_{1}.dump'.format(host,database,user,schema)
    runprocess(password)
    
elif 'schema' in requirement and 'all' in requirement:
    command = 'pg_dump -h {0} -U {2} -p 5432 -d {1} -n {1}.* -Fc -f pg_allschemas.dump'.format(host,database,user)
    runprocess(password)
    
elif 'only' in requirement and 'schema' in requirement:
    command = 'pg_dump -h {0} -U {2} -p 5432 -d {1} -n {3} -Fc -f pg_{3}_schema.dump'.format(host,database,user,schema)
    runprocess(password)
    
elif 'table' in requirement and 'all' in requirement:
    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -t {3}.* -Fc -f pg_{3}_allTable.dump'.format(host,database,user,schema)
    runprocess(password)
    
elif 'only' in requirement and 'table' in requirement:
    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -t {3}.{4} -Fc -f pg_{3}_{4}.dump'.format(host,database,user,schema,table)
    runprocess(password)   

else:
    print("requirement is not match")

    
    
session = boto3.Session(
       aws_access_key_id=os.getenv("AWS_ACCESS"),
       aws_secret_access_key=os.getenv("AWS_SECRET"),
     )
s3 = session.resource('s3')
dump_files = glob.glob("pg_*.dump")

for filename in dump_files:
    key = "%s/%s" % ('backup', os.path.basename(filename))
    print("Putting %s as %s" % (filename,key))
    s3.meta.client.upload_file(filename, bucketname, key)


os.system("rm pg*")
    
    

#!/usr/bin/python

import glob
import os
import boto3
import re
from subprocess import PIPE,Popen

#taking input from teamcity environment
requirement = os.getenv("requirement")
host        = os.getenv("host_url")
database    = os.getenv("database_name")
user        = os.getenv("user_name")
password    = os.getenv("database_passwd")
schema      = os.getenv("schema_name")
table       = os.getenv("table_name")
bucketname  = os.getenv("bucket_name")
dt = "$(date +'%d-%m-%Y-%H:%M:%S')"

#remove all existing backup file from current directory
os.system("rm pg*")

#To take the backup you need to first authenticate the database
def authenticate(password):

    p = Popen(command, shell=True, env={**os.environ, "PGPASSWORD": password})
    p.communicate('{}\n'.format(password))

# if first word "only" and last word "database" are match in requirement then this conditon will run otherwise move on elif condition

if re.search("^only.*database$", requirement):
    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -Fc -f pg_{1}_{3}.dump'.format(host,database,user,dt)
    authenticate(password)
    
# if first word "all" and last word "schema" are match in requirement then this conditon will run otherwise move on elif condition

elif re.search("^all.*schema$", requirement):
    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -n {1}.* -Fc -f pg_{1}_{3}.dump'.format(host,database,user,dt)
    authenticate(password)

elif re.search("^only.*schema$", requirement):
    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -n {3} -Fc -f pg_{3}_{4}.dump'.format(host,database,user,schema,dt)
    authenticate(password)

elif re.search("^all.*table$", requirement):
    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -t {3}.* -Fc -f pg_{3}_{4}.dump'.format(host,database,user,schema,dt)
    authenticate(password)

elif re.search("^only.*table$", requirement):
    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -t {3}.{4} -Fc -f pg_{4}_{5}.dump'.format(host,database,user,schema,table,dt)
    authenticate(password)

    
# if nothing is match then else part will run
else:
    print("requirement is not match")



# make connection with AWS
session = boto3.Session(
       aws_access_key_id=os.getenv("AWS_ACCESS"),
       aws_secret_access_key=os.getenv("AWS_SECRET"),
     )

#assign resource name to session
s3 = session.resource('s3')
dump_files = glob.glob("pg_*.dump")

#upload multiple file on s3 which is in current directory
for filename in dump_files:
    key = "%s/%s" % ('backup', os.path.basename(filename))
    print("Putting %s as %s" % (filename,key))
    s3.meta.client.upload_file(filename, bucketname, key)

#after upload backup file in s3 then delete backup file from current directory
os.system("rm pg*")

    

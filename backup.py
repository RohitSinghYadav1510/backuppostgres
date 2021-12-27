#!/usr/bin/python

import glob
import os
import boto3
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


#remove all existing backup file from current directory
os.system("rm pg*")

#To take the backup you need to first authenticate the database
def passwdpro(password):

    p = Popen(command, shell=True, env={**os.environ, "PGPASSWORD": password})
    p.communicate('{}\n'.format(password))

# if "only database" string available in requirement then this conditon will run otherwise move on elif condition
if 'only database' in requirement:
    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -Fc -f pg_Onlydatabase.dump'.format(host,database,user)
    passwdpro(password)

elif 'all schema' in requirement:
    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -n {1}.* -Fc -f pg_allschemas.dump'.format(host,database,user)
    passwdpro(password)

elif 'only schema' in requirement:
    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -n {3} -Fc -f pg_Onlyschema.dump'.format(host,database,user,schema)
    passwdpro(password)

elif 'all table' in requirement:
    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -t {3}.* -Fc -f pg_allTables.dump'.format(host,database,user,schema)
    passwdpro(password)

elif 'only table' in requirement:
    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -t {3}.{4} -Fc -f pg_Onlytable_{4}.dump'.format(host,database,user,schema,table)
    passwdpro(password)

 # if condition is not match then else part will run
else:
    print("requirement is not match")


# make connection with AWS
session = boto3.Session(
       aws_access_key_id=os.getenv("AWS_ACCESS"),
       aws_secret_access_key=os.getenv("AWS_SECRET"),
     )

#assign resources name to session
s3 = session.resource('s3')
dump_files = glob.glob("pg_*.dump")

#upload multiple file on s3 which is in current directory
for filename in dump_files:
    key = "%s/%s" % ('backup', os.path.basename(filename))
    print("Putting %s as %s" % (filename,key))
    s3.meta.client.upload_file(filename, bucketname, key)

#after upload backup file in s3 then delete backup file from current directory
os.system("rm pg*")

    

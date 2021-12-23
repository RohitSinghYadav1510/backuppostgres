#!/usr/bin/python

import os
from subprocess import PIPE,Popen

requirement = os.getenv("requirement")
host        = os.getenv("host_url")
database    = os.getenv("database_name")
user        = os.getenv("user_name")
password    = os.getenv("PWD")
schema      = os.getenv("schema_name")
table       = os.getenv("table_name")

def run(password):
    p = Popen(command, shell=True, env={**os.environ, "PGPASSWORD": password})
    p.communicate('{}\n'.format(password))
  

if 'only' in requirement and 'database' in requirement:
    command = 'pg_dump -h {0} -U {2} -p 5432 -d {1} -Fc -f pg_{1}.dump'.format(host,database,user,schema)
    run(password)
    
elif 'schema' in requirement and 'all' in requirement:
    command = 'pg_dump -h {0} -U {2} -p 5432 -d {1} -n {1}.* -Fc -f pg_allschemas.dump'.format(host,database,user)
    run(password)
    
elif 'only' in requirement and 'schema' in requirement:
    command = 'pg_dump -h {0} -U {2} -p 5432 -d {1} -n {3} -Fc -f pg_{3}_schema.dump'.format(host,database,user,schema)
    run(password)
    
elif 'table' in requirement and 'all' in requirement:
    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -t {3}.* -Fc -f pg_{3}_allTable.dump'.format(host,database,user,schema)
    run(password)
    
elif 'only' in requirement and 'table' in requirement:
    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -t {3}.{4} -Fc -f pg_{3}_{4}.dump'.format(host,database,user,schema,table)
    run(password)   

else:
    print("requirement is not match")


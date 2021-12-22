#!/usr/bin/python

import os
from subprocess import PIPE,Popen

requirement = os.getenv("input")
host     = '172.18.0.2'
database = 'postgres'
user     = 'postgres'
schema   = 'public'
table    = 'company'
password = 'example'


if 'only' in requirement and 'database' in requirement:
    command = 'pg_dump -h {0} -U {2} -p 5432 -d {1} -Fc -f pg_{1}.dump'.format(host,database,user,schema)

elif 'schema' in requirement and 'all' in requirement:
    command = 'pg_dump -h {0} -U {2} -p 5432 -d {1} -n {1}.* -Fc -f pg_allschemas.dump'.format(host,database,user)

elif 'only' in requirement and 'schema' in requirement:
    command = 'pg_dump -h {0} -U {2} -p 5432 -d {1} -n {3} -Fc -f pg_{3}_schema.dump'.format(host,database,user,schema)

elif 'table' in requirement and 'all' in requirement:
    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -t {3}.* -Fc -f pg_{3}_allTable.dump'.format(host,database,user,schema)

elif 'only' in requirement and 'table' in requirement:
    command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -t {3}.{4} -Fc -f pg_{3}_{4}.dump'.format(host,database,user,schema,table)

else:
    print("requirement is not match")



p = Popen(command, shell=True, env={**os.environ, "PGPASSWORD": password})
p.communicate('{}\n'.format(password))

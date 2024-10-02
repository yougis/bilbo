#This piece of code will create the yml and sql files needed in a postgresql dbt project
#from the postgres DB. It's reversing engineering the files so they don't need to be 
#created manually.
from oeilnc_config.settings import usr, pswd, host, port, db_traitement
import os
import psycopg2 
import pandas as pd
import shutil
from pathlib import Path

from datetime import date, datetime, timedelta
from timeit import default_timer as timer 

# Database Server details
sql_host = host
sql_user = usr
sql_password = pswd
sql_db = db_traitement
 
# Directories 
models_dir = Path('./models')
source_schemas = ['processing']

# filtrer les tables par les nom
start_with = 'faits_'
not_end_with = 'withError'

#dates
today = date.today()
now = datetime.now()
 
global connpg

def sql_to_df(sql_query, column_names):

    curpg = connpg.cursor() 
    try:
      curpg.execute(sql_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print('Error: ', error)
        return 1
 
   # The execute returns a list of tuples:
    tuples_list = curpg.fetchall()
    curpg.close() 
   # Now we need to transform the list into a pandas DataFrame:
    df = pd.DataFrame(tuples_list, columns=column_names)
    
    curpg.close() 
    
    return df

def get_metadata(): 
    
    sql_postgres = "select pg_namespace.nspname as schema,\
                    pg_class.relname as object_name, \
                    case when pg_class.relkind in ('v','m') then 'view'\
                        when pg_class.relkind = 'r' then 'table' end\
                        as object_type, \
                    pg_attribute.attname AS column_name,\
                    replace(replace(pg_catalog.format_type(pg_attribute.atttypid, pg_attribute.atttypmod),\
                                    'character varying','varchar'),\
                                    'timestamp without time zone','timestamp') AS data_type,\
                    case when pg_attribute.attnotnull= true then 'not null' else 'null' end mandatory\
                    FROM\
                        pg_catalog.pg_attribute\
                    left join\
                        pg_catalog.pg_class ON (pg_class.oid = pg_attribute.attrelid\
                                                and pg_class.relkind in ('r','v','m'))\
                    inner join \
                        pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace\
                    left join \
                        pg_catalog.pg_type on pg_type.oid = pg_class.reltype \
                    WHERE\
                        pg_attribute.attnum > 0\
                        AND NOT pg_attribute.attisdropped\
                        and  pg_namespace.nspname not in ('information_schema',\
                                                            'pg_catalog',\
                                                            'pg_toast')\
                    ORDER BY\
                        schema, pg_class.relname asc, attnum ASC;"
    
    pg_md = sql_to_df(sql_postgres,('schema','object_name','object_type','column_name','data_type','mandatory'))
    pg_md = pg_md[pg_md['object_name'].str.startswith(start_with)]
    pg_md = pg_md[~pg_md['object_name'].str.endswith(not_end_with)]
    print(pg_md)
    
    return pg_md

def create_schema_dirs(df):
    #This will create each schema directory under models dir. 
    print(df)
    for sch in df:
        try:
            path = Path(models_dir/str(sch+'_src'))
            print(path)
            os.mkdir(path) 
        except FileExistsError:
             pass 
        except Exception as e:
            print(str(e))
            return 1
    return 0
        
def create_source_yml_files(fdf):
    #This will create the different YML  files for each object
    for scrow in fdf.schema.unique():
        npath = Path(models_dir/str(scrow+'_src'))
        print(npath)
        for obrow in (fdf[fdf['schema'] ==scrow].object_name).unique():
            print('---object name: '+obrow)
            for obtrow in (fdf[(fdf['schema'] ==scrow) & (fdf['object_name'] ==obrow)].object_type).unique():
                print('     ---object type:'+obtrow)
                filename = obrow+'.yml'
                with open( Path(npath/filename), 'w') as fp:
                    fp.write('version: 2\n')
                    fp.write('sources:\n')
                    fp.write('  - name: '+sql_db+'-'+scrow+'\n')
                    fp.write('    database: '+sql_db+'\n')
                    fp.write('    schema: '+scrow+'\n')      
                    fp.write('    tables: \n')                   
                    fp.write('      - name: '+obrow+'\n')
                     
                    fp.write('        columns:\n')
                    for coli,colrow in fdf[(fdf['schema'] ==scrow) & (fdf['object_name'] ==obrow) & (fdf['object_type'] ==obtrow)].iterrows() :
                        print(colrow['schema']+'---'+colrow['object_name']+'---'+colrow['column_name']+'---'+colrow['data_type']+'---'+colrow['mandatory']) 
                        fp.write('          - name: '+colrow['column_name']+'\n')

def create_source_model_files(fdf):
    #This will create the different SQL files for each object
    for scrow in fdf.schema.unique():
        npath = Path(models_dir/str(scrow+'_src'))
        print(npath)
        for obrow in (fdf[fdf['schema'] ==scrow].object_name).unique():
            print('---object name: '+obrow)
            for obtrow in (fdf[(fdf['schema'] ==scrow) & (fdf['object_name'] ==obrow)].object_type).unique():
                print('     ---object type:'+obtrow)
                filename = obrow+'.sql'
                with open( Path(npath/filename), 'w') as fp:
                    fp.write('with source_data as (\n')
                    fp.write('      select * \n')
                    fp.write('      from {{ source(\''+sql_db+'-'+scrow+'\',\''+obrow+'\') }} \n')
                    fp.write(')\n')
                    fp.write('\n')
                    fp.write('select *\n')      
                    fp.write('from source_data \n') 
  
          
if __name__ == "__main__":
    #create db connection
    connpg = psycopg2.connect(database=sql_db, user = sql_user, password = sql_password , host = sql_host, port = "5432") 
    print ("Opened database successfully")
    #Connect to the DB and collect all the metadata for the tables and views in the DB
    pg_md = get_metadata()
    #create the dirs for the source tables and views
    succ = create_schema_dirs(pg_md[(pg_md['schema'].isin(source_schemas))].schema.unique())
    print(succ)
    if succ == 0:
        #this will create the source reference in the DBT project:
        create_source_yml_files(pg_md[(pg_md['schema'].isin(source_schemas))])
        #This will create the sql files referencing the sources:
        create_source_model_files(pg_md[(pg_md['schema'].isin(source_schemas))])
        

import json
from io import StringIO
import psycopg2
import pandas as pd
import getpass

class simple_db():
    '''
    A simple Python class for managing connections to a PostgreSQL database.
    
    This class provides methods for connecting to a PostgreSQL database, executing queries,
    creating temporary tables, copying data from DataFrames to database tables, and managing
    the database connection.

    Class Attributes:
        PANDAS_TO_PG_DTYPES (dict): A dictionary to map pandas dtypes to postgres datatypes
        PG_TO_PANDAS_DTYPES (dict): A dictionary to map postgres datatypes to pandas dtypes, uses PG OIDs

    Instance Attributes:
        self.__host__ (str): The hostname for the pg connection
        self.__database__ (str): The database for the pg connection
        self.__user__ (str): The user for the pg connection
        self.conn (psycopg2.connection): The connection object for the pg connection
        self.role (str): The role to use for querying and writing
        self.default_write_schema (str|None)=None: The default schema to which to write tables
        self.current_temp_tables (list): A list of temporary tables associated with the current connection
    '''

    PANDAS_TO_PG_DTYPES = {'object':'VARCHAR(256)', 'string':'VARCHAR(256)',
        'int64':'BIGINT', 'float64':'FLOAT', 'bool':'BOOLEAN',
        'datetime64[ns]':'TIMESTAMP', 'timedelta64[ns]':'INTERVAL',
        'datetime64':'DATE', 'datetime64[ns, UTC]':'TIMESTAMP WITH TIME ZONE',
        'Int64': 'BIGINT'}
    PG_TO_PANDAS_DTYPES = pg_oid_to_pandas_dtypes = {
        25: 'string',                # OID for TEXT
        18: 'string',                # OID for CHAR
        23: 'Int64',                 # OID for INTEGER
        20: 'Int64',                 # OID for BIGINT
        21: 'Int64',                 # OID for SMALLINT
        700: 'float64',              # OID for FLOAT
        701: 'float64',              # OID for REAL
        701: 'float64',              # OID for DOUBLE PRECISION
        16: 'bool',                  # OID for BOOLEAN
        1114: 'datetime64[ns]',      # OID for TIMESTAMP
        1082: 'datetime64[ns]',      # OID for DATE
        1083: 'datetime64[ns]',      # OID for TIME
        1184: 'datetime64[ns, UTC]', # OID for TIMESTAMP WITH TIME ZONE
        1186: 'timedelta64[ns]',     # OID for INTERVAL
        1700: 'float64'}             # OID for NUMERIC

    def __init__(self):
        self.__host__ = input('Host:')
        self.__database__ = input('Database:')
        self.__user__ = input('User:')
        self.conn = self.remote_connect(self.__host__, self.__database__, self.__user__)
        self.role = self.role_change(input('Role: '))
        self.default_write_schema = None
        self.current_temp_tables=[]

    def __clean_creds__(self, credentials_dict:dict):
        '''
        @NOTE This is around for if people want to connect using srcr files
        '''
        cleaned_dict={}
        for k in credentials_dict.keys():
            if k in ['host','port','dbname','user']:
                cleaned_dict[k]=credentials_dict[k]
        return cleaned_dict

    def __remote_connect_old__(self, json_path:str, pw:str):
        '''
        has hardcoded keys from argos rather than arbitrary ones for now
        @TODO set it up with uri string or a .ini instead, accept only pg named arguments
        (ie no dbname, nor src_args, etc.)
        '''
        with open(json_path) as db_creds:
            creds=json.load(db_creds)['src_args']
        clean_creds=self.__clean_creds__(creds)
        return psycopg2.connect(host=clean_creds['host'],
                 database=clean_creds['dbname'],
                 user=creds['user'],
                 password=pw)
    
    def remote_connect(self, host:str, database:str, user:str,from_json=False):
        '''
        Connect to a given postgres database
        '''
        pw = getpass.getpass('Password: ')
        if not from_json:
            return psycopg2.connect(host=host,database=database,user=user,password=pw)
        else:
            return self.__remote_connect_old__(input('json_path:'),pw)

    def role_change(self,role:str):
        '''
        Change your role on the database for the current connection.

        Parameters:
            role (str): the role to which you want to set your connection
        '''
        with self.conn.cursor() as cur:
            try:
                cur.execute('SET ROLE {}'.format(role))
                self.conn.commit()
            except Exception as e:
                print(e)
                self.conn.rollback()
                return e
    
    def output_tbl(self, query:str, tbl_name:str, schema_name:str|None=None,
        params:list|dict|None=None, temporary:bool=False, overwrite:bool=False):
        '''
        Outputs a table to the database. A temporary table will be schemaless, regardless of the schema argument.
        Temporary tables will only overwrite when the tbl_name is present in the current_temp_tables attribute.
        
        Parameters:
            query (string): A query string to execute, optionally accepts parameterization
            tbl_name (str): The name of the table to write
            schema_name (str|None)=None: A schema to qualify the table.  If schema_name is None and temporary is False,\
                the default_write_schema attribute is used.  If temporary is True, schema_name is ignored
            params (list|dict|None)=None: Optional parameters to abstract the query string
            temporary (bool): Whether the table should be a TEMP TABLE
            overwrite (bool): Whether to overwrite an existing table of the same name.  If temporary=True and a table of\
                the same name is not present in the current_temp_tables attribute, overwrite will be set to False
        '''
        temp=''
        drop_tbl = ''

        if not schema_name:
            schema_name=self.default_write_schema+'.'
        else:
            schema_name+='.'

        if temporary:
            temp=' TEMP'
            schema_name=''

        if overwrite:
            if temp and not tbl_name in self.current_temp_tables:
                print(f'Temp table not in current temp table list, overwrite set to False')
            else:
                drop_tbl=f'DROP TABLE IF EXISTS {schema_name}{tbl_name}; '
        
        full_string = f"{drop_tbl}CREATE{temp} TABLE {schema_name}{tbl_name} as ("+query+")"
        
        self.query_no_return(full_string,params)
        
        if temp:
            self.current_temp_tables.append(tbl_name)

    def query_no_return(self, query: str, params: list|dict|None=None):
        '''
        Execute a query that does not return any data locally

        Parameters:
            query (string): A query string to execute. Accepts parameterization.
            params (list|dict|None): Defaults to None. Optionally provide a list or dictionary with params for the query string
        '''
        with self.conn.cursor() as cur:
            try:
                if params:
                    cur.execute(query, params)
                else:
                    cur.execute(query)
                self.conn.commit()
                print('execution complete')

            except (Exception, psycopg2.DatabaseError) as e:
                print(e)
                self.conn.rollback()
                return e
    
    
    def query_return_df(self,query: str,params: list|dict|None=None):
        '''
        Executes a query and collects the results locally as a pd.DataFrame()
        It will attempt to map PG to PD DTypes, values that cannot be mapped are mapped as dtype.object

        Parameters:
            query (string): A query string to execute. Accepts parameterization.
            params (list|dict|None): Defaults to None. Optionally provide a list or dictionary with params for the query string
        
        Returns: 
            A pd.DataFrame with the query results
        '''
        
        with self.conn.cursor() as cur:
            try:
                if params:
                    cur.execute(query, params)
                else:
                    cur.execute(query)
    
                df_dtypes = {desc[0]:simple_db.PG_TO_PANDAS_DTYPES.get(desc[1],'object') for desc in cur.description}

                df=pd.DataFrame(cur.fetchall(),
                    columns=[desc[0] for desc in cur.description]).astype(df_dtypes)
                
                self.conn.commit()
                return df
            except Exception as e:
                print(e)
                self.conn.rollback()
                return e

    def bulk_insert_stringio(self, df, tbl_name):
        '''
        write an in memory csv (avoiding disk!) and copy it to an existing database table

        @TODO chunk this to accomodate larger tables, maybe 250k default chunk size?
        '''
        buffer = StringIO()
        buffer.write(df.to_csv(header=True, index=False))
        buffer.seek(0)
        with self.conn.cursor() as cur:
            try:
                load_string = f"COPY {tbl_name} FROM STDIN DELIMITER ',' CSV HEADER;"
                cur.copy_expert(load_string,buffer)
                self.conn.commit()
            except (Exception, psycopg2.DatabaseError) as e:
                self.conn.rollback()
                print(e)
                return e
    
    def create_tbl_shell_from_df(self, df, tbl_name, temporary=False, overwrite=False):
        '''
        Creates a table in postgres, if the table is schema qualified it will persist and
        if the table is not schema qualified, it will be temporary
        '''
        with self.conn.cursor() as cur:
            try:
                drop_tbl=''
                temp=''
                columns = []
                for column_name, dtype in df.dtypes.items():
                    postgres_type = simple_db.PANDAS_TO_PG_DTYPES.get(str(dtype), 'VARCHAR')
                    columns.append(f"{column_name} {postgres_type}")
                
                if temporary:
                    temp=' TEMP'
                if overwrite and ((temporary and tbl_name in self.current_temp_tables) or not temporary):
                    drop_tbl=f'DROP TABLE IF EXISTS {tbl_name}; '
                
                # Construct CREATE TABLE query
                create_table_query = f"{drop_tbl}CREATE{temp} TABLE {tbl_name} ({', '.join(columns)})"
                cur.execute(create_table_query)
                if temp:
                    self.current_temp_tables.append(tbl_name)
            except (Exception, psycopg2.DatabaseError) as e:
                self.conn.rollback()
                print('Unable to create table: ')
                print(e)
                return e

    def copy_df_to_pg(self,df,tbl_name,schema_name=None,temporary=False, overwrite=False):
        '''
        Copies a dataframe to a new table in PG.  
        '''
        with self.conn.cursor() as cur:
            try:
                if not schema_name:
                    schema_name=self.default_write_schema+'.'
                else:
                    schema_name+='.'
                
                if not temporary:
                    tbl_name=f'{schema_name}{tbl_name}'
                
                self.create_tbl_shell_from_df(df,tbl_name,temporary, overwrite)
                self.bulk_insert_stringio(df,tbl_name)
            except (Exception, psycopg2.DatabaseError) as e:
                self.conn.rollback()
                print('Unable to copy table: ')
                print(e)
                return e

    def close_connection(self):
        # @TODO figure out where to close automatically, if ever. 
        self.conn.close()
        print('this instance is no longer functional, the connection is permanently closed')



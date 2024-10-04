from sqlalchemy import create_engine, MetaData, Table, inspect, text, select
from sqlalchemy import inspect
from dotenv import load_dotenv
import os

class DatabaseManager:
    def __init__ (self):
        self.user, self.password, self.host, self.port, self.db = self._get_keys()
        self.engine = create_engine(f"mysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}")
        self.metadata = MetaData()
        self.connection = self.engine.connect()
    
    def _get_keys(self):
        load_dotenv('keys.env')
        return os.getenv('USER'), os.getenv('PASSWORD'), os.getenv('HOST'), os.getenv('PORT'), os.getenv('DBNAME')
    
    #self.engine.table_names() is depreciated. This the moder way of getting database table names
    def get_tables_names_inspector(self):
        inspector = inspect(self.engine)
        return inspector.get_table_names()
    
    #getting table metadata
    def table_from_metadata(self,table):
        table_obj = Table(table, self.metadata, autoload_with=self.engine) #autoload is key, as it queries the database itself 
        return table_obj
    
    def column_names(self,table_obj):
        return table_obj.columns.keys()
    
    def select(self):
        tables = self.get_tables_names_inspector()
        for table in tables:
            select_query_normal_sql = f"SELECT * FROM {table} LIMIT 3"
            select_query_sqlalchemy_way = select(self.table_from_metadata(table)).limit(3)
            
            result_normal_sql = self.connection.execute(text(select_query_normal_sql)).fetchall()
            result_sqlalchemy_way = self.connection.execute(select_query_sqlalchemy_way).fetchall()
            
            print(f"""{table} selection normal sql way: {result_normal_sql} 
{table} selection sqlalchemy way: {result_sqlalchemy_way} \n """)

def main():
    dbmanager1 = DatabaseManager()
    print(dbmanager1.get_tables_names_inspector())
    machine_table = dbmanager1.table_from_metadata('machinedata')
    print(f"metadata of {repr(machine_table)} \n")
    print(f"column names of machine data {dbmanager1.column_names(machine_table)}")
    
    dbmanager1.select()
    
    
if __name__ == '__main__':
    main()

    
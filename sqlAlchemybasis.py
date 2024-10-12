from sqlalchemy import Select, create_engine, MetaData, Table, inspect, text, select
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
class Tables(DatabaseManager): 

    def __init__(self):
        super().__init__()
        self.table_names = self.get_tables_names_inspector()
        self.table_metadata,self.column_names = self.table_from_metadata()
    
    def get_tables_names_inspector(self):
        inspector = inspect(self.engine)
        return inspector.get_table_names()
    
    
    def table_selection(self, table = ""):
        while table not in self.table_names:
            table = input(f"Escoja la tabla sobre la que quiere trabajar ({self.table_names}): ")
            self.working_on_table = table
        return self.working_on_table
    
    #getting table metadata/reflectin the table
    def table_from_metadata(self):
        table_metadata = Table(self.table_selection(), self.metadata, autoload_with=self.engine) #autoload is key, as it queries the database itself 
        column_names = table_metadata.columns.keys()
        return table_metadata,column_names
    
    
class Selects(Tables):

    def __init__(self):
        super().__init__()
        self.table_selected = self.table_selection()
        self.basic_select = self.normal_select()

        
    def normal_select(self):
        
        select_query_normal_sql = f"SELECT * FROM {self.table_selected} LIMIT 3"
        select_query_sqlalchemy_way = select(text(self.table_selected)).limit(3)
        
        result_normal_sql = self.connection.execute(text(select_query_normal_sql)).fetchall() #without fetchall this jus ResultProxy obj, but with fetchall is the ResultSet obj containing the data. List of touples
        result_sqlalchemy_way = self.connection.execute(select_query_sqlalchemy_way).fetchall()
        print(f"Table: {self.table_selected}\n")
        print(f"Normal SQL Query Result (First 3 rows): {result_normal_sql}")
        print(f"SQLAlchemy Query Result (First 3 rows): {result_sqlalchemy_way}\n")
        print("-" * 40)

    
    def where_select(self):
        table_herramienta = self.table_from_metadata('herramienta')
        columns = table_herramienta.columns.keys()
        print(columns)
        
def main():
   #dbmanager1 = DatabaseManager()
   #tables = Tables('machinedata')
   #print(f"table names {tables.get_tables_names_inspector()}")
   #machine_table = tables.table_from_metadata()
   #print(f"metadata of {repr(machine_table)} \n")
    
    table = Tables()
    
    
    #select_query = Selects()
    #print(select_query.table_selected)
    #select_query.basic_select
    #select_query.where_select()
    
if __name__ == '__main__':
    main()

    
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy import inspect
from dotenv import load_dotenv
import os

class DatabaseManager:
    def __init__ (self):
        self.user, self.password, self.host, self.port, self.db = self._get_keys()
        self.engine = create_engine(f"mysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}")
        self.metadata = MetaData()
    
    def _get_keys(self):
        load_dotenv('keys.env')
        return os.getenv('USER'), os.getenv('PASSWORD'), os.getenv('HOST'), os.getenv('PORT'), os.getenv('DBNAME')
    
    def get_tables_inspector(self):
        inspector = inspect(self.engine)
        return inspector.get_table_names()
    
    def table_from_metadata(self,table):
        table_obj = Table(table, self.metadata, autoload_with=self.engine)
        return table_obj
        
        
        
if __name__ == '__main__':
    dbmanager1 = DatabaseManager()
    print(dbmanager1.get_tables_inspector())
    machine_table = dbmanager1.table_from_metadata('machinedata')
    print(repr(machine_table))
    
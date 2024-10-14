from ast import While
from matplotlib import legend
from pyparsing import col
from sqlalchemy import Select, create_engine, MetaData, Table, func, inspect, text, select, and_, or_, not_, desc, case
from sqlalchemy import inspect
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os

class DatabaseManager:
    def __init__ (self):
        self.user, self.password, self.host, self.port, self.db = self._get_keys()
        self.engine = create_engine(f"mysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}")
        self.metadata = MetaData()
        self.connection = self.engine.connect()
     
    def result_proxy(self,stmt):
        return self.connection.execute(stmt)
    
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
        self.basic_select = self.normal_select()
        self.select_where = self.where_select()
       
            
        
        
    def normal_select(self):
        
        select_query_normal_sql = f"SELECT * FROM {self.table_metadata} LIMIT 3"
        select_query_sqlalchemy_way = select(self.table_metadata).limit(3)
        
        result_normal_sql = self.connection.execute(text(select_query_normal_sql)).fetchall() #without fetchall this jus ResultProxy obj, but with fetchall is the ResultSet obj containing the data. List of touples
        result_sqlalchemy_way = self.connection.execute(select_query_sqlalchemy_way).fetchall()
        print(f"Table: {self.table_metadata}\n")
        print(f"Normal SQL Query Result (First 3 rows): {result_normal_sql} \n")
        print(f"SQLAlchemy Query Result (First 3 rows): {result_sqlalchemy_way}\n")
        print("-" * 40)


    def where_select(self):
        general_stmt = select(self.table_metadata)
        if self.working_on_table == 'herramienta':
            stmt = general_stmt.where(self.table_metadata.columns.n_reafilados >= 2).limit(3)
            results = self.connection.execute(stmt).fetchall()
            return f"First three rows of herramienta with nºreafilados > 2: {results}"  
        
        elif self.working_on_table == 'machinedata':
            df = pd.DataFrame()
            stmt = general_stmt.where(self.table_metadata.columns['OF'].in_(['47884049']))
            result_proxy = self.connection.execute(stmt)
            while True:
                chunk = result_proxy.fetchmany(500)
                if not chunk:
                    break
                batch_df = pd.DataFrame(chunk, columns = self.column_names)
                df = pd.concat([df, batch_df], ignore_index= True)
            return df

        elif self.working_on_table == 'pieza':
            stmt = general_stmt.where(or_(self.table_metadata.columns.of == '47315924', self.table_metadata.columns.resultado_medicion != 0))
            results = self.connection.execute(stmt).fetchall()
            return f"Or statement results: {results}"  

    def aggregate_and_short_data(self):
        
        def plot_data(df):
            df.head(10).plot.bar(x = 'id_herramienta', y = 'diametro_herramienta') #legend is the indicative of the color in the plot
            plt.xlabel("ID Herramienta")
            plt.ylabel("Diámetro Herramienta")
            plt.title("Diámetro de herramienta por ID")
            plt.show()
            
        def data_options():
            while True:
                option = int(input("Choose 1 for aggregated sum (1 col), 2 for normal sum (2 col), 3 for groupby and other for passing"))
                
                if option == 1:
                    stmt = select(func.sum(self.table_metadata.columns.n_puntas)).where(self.table_metadata.columns.fabricante == 'WALTER')
                    self.opt1_sum = self.connection.execute(stmt).fetchall()[0][0]
                    return f"Nº_puntas needed for WALTER tools {self.opt1_sum}"
                
                if option == 2:
                    stmt = select(self.table_metadata.columns.id_herramienta, self.table_metadata.columns.herramienta, 
                                ((self.table_metadata.columns['LTH']-self.table_metadata.columns['LV'])/self.table_metadata.columns['LTH']*100).label("Cutting percent"))\
                                    .where(and_(self.table_metadata.columns.tipo_rosca == "NO ROSCA", self.table_metadata.columns['LTH'] != 0))
                    self.opt2_percentajeLV = self.connection.execute(stmt).fetchall()
                    df = pd.DataFrame(self.opt2_percentajeLV, columns = ['id_herramienta','Tipo Hta','Cutting percent'])
                    return df
                else:
                    print("opción no válida")           
            
            
        if self.working_on_table == 'herramienta':
            data_options()
            stmt = select(self.table_metadata.columns.id_herramienta, 
                          self.table_metadata.columns.diametro_herramienta).order_by(desc(self.table_metadata.columns.diametro_herramienta)) #select id_herramienta, hora from herramienta order by diametro_herramienta asc;
            results = self.connection.execute(stmt).fetchall()
            df = pd.DataFrame(results, columns = ['id_herramienta', 'diametro_herramienta'])
            #df = df.loc[:,['id_herramienta', 'diametro_herramienta']] #all rows and these two columns
            df.dropna(subset=['diametro_herramienta'])
            df['diametro_herramienta'] = pd.to_numeric(df['diametro_herramienta'], errors = 'coerce')
            plot_data(df)
            return df
        
        if self.working_on_table == 'machinedata':
            stmt = select(self.table_metadata).order_by(self.table_metadata.columns['OF'], desc(self.table_metadata.columns.hora)) #if of is the same, order by desc 'hora': select * from machinedata order by `OF`, hora desc;
            results = self.connection.execute(stmt).fetchall()
            df = pd.DataFrame(results, columns=self.column_names)
            return df
        
        if self.working_on_table == 'pieza':
            stmt = select(func.count(case(
            (self.table_metadata.columns.resultado_medicion != '0', 1),
            else_=None
          )))
            results = self.connection.execute(stmt).fetchall()
            stmt1 = select(self.table_metadata.columns.resultado_medicion).where(self.table_metadata.columns.resultado_medicion != '0')
            results1= self.connection.execute(stmt1).fetchall()
            
            return f"Number of measurements presenting an error = {results[0][0]}, and the errors are = {results1}"
    
def main():
    #dbmanager1 = DatabaseManager()
    #tables = Tables('machinedata')
    #print(f"table names {tables.get_tables_names_inspector()}")
    #machine_table = tables.table_from_metadata()
    #print(f"metadata of {repr(machine_table)} \n")
    
    select1 = Selects()
    select1.basic_select
    #print(select1.where_select())
    print(select1.aggregate_and_short_data())
    #rint(select1.opt1_sum)
    print(select1.opt2_percentajeLV)
    #select_query = Selects()
    #print(select_query.table_selected)
    #select_query.basic_select
    #select_query.where_select()
    print("jdfk")
    
if __name__ == '__main__':
    main()

    
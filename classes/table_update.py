from sqlalchemy import create_engine
from dotenv import dotenv_values

config = dotenv_values(".env")

USER = config['USER']
PASS = config['PASSWORD']
HOST = config['HOST']
DATABASE = config['TO_DATABASE']
PORT = '3306'

engine = create_engine('mysql+mysqlconnector://'+USER+':'+PASS+'@localhost:'+PORT+'/'+DATABASE, echo=False)

class FinalUpdate:
    def __init__(self, final_data):
        self.final_data = final_data

    def table_insert(self):
        #self.final_data.to_sql(con=engine, name='sales_aggregate', if_exists='replace')
        self.final_data.to_sql(con=engine, name='sales_aggregate', if_exists='append')

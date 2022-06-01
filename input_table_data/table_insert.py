from classes.connection import Connection
from classes.execute_sql import ExecuteSql
from dotenv import dotenv_values
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

config = dotenv_values(os.path.join(BASE_DIR, '.env'))

'''
CREATE TABLE <db_name>.`online_sales` (
    customer_id int,
    firstname char(30),
	lastname char(30),
	contact_no char(15), 
    email_id varchar(80),
	primary_address varchar(255),
	secondary_address varchar(255),
    policy_type char(30),
	policy_name varchar(255),
	regular_direct char(30),
	unit_value int,
	units int,
	purchase_value  int,
	purchase_date date
);

'''



connection = Connection( USER = config['USER']
                        , PASS = config['PASSWORD']
                        , HOST = config['HOST']
                        , DATABASE = config['FROM_DATABASE'] )


executesql = ExecuteSql( connection = connection)

sql_insert = """insert into online_sales ( customer_id, firstname, lastname, contact_no, email_id, 
primary_address, secondary_address, policy_type, policy_name, regular_direct, unit_value, units, 
purchase_value, purchase_date ) VALUES """
sql_insert += """ ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
params = ('21', 'Andrew', 'Garfield', '9182736450', 'andy@temp.com', 'Denver, Colorado, 80206', '', 'Lumpsum',
          'Tech Fund', 'Regular', '75', '80', '6000', '2022-05-30')

executesql.insert_records(sql_insert, params)



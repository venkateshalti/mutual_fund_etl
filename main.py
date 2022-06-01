# this program serves as the driver for the entire ETL process
# we will extract data from json files, rest services, csv files and database

import json  # we need this package to read the json file
from dotenv import dotenv_values  # this is used to read data from .env file
import pandas as pd

from classes.file_management import FileManagement  # this class has steps to download and unpack rar files
from classes.parser_xml import ParserXML  # this will parse the XML to extract values
from classes.table_update import FinalUpdate  # importing FinalUpdate class, to load final df to output table
from classes.df_transform import Transform  # importing Transform class that performs transforms the dataframe

with open('./states.json', 'r') as file:  # created IO class called file that can be extracted
    state_codes = json.load(file)  # created a dictionary that holds all the state code information

filemanagement = FileManagement()
config = dotenv_values(".env")
# downloading rar file from URL and storing to location from config file
print("downloading rar file from URL and storing to location from config file")
filemanagement.downloadRequestRarURL( REQUEST_DOWNLOAD_URL = config['REQUEST_DOWNLOAD_URL']
                                        , DOWNLOAD_LOCATION = config['DOWNLOAD_LOCATION'] )
# unrar downloaded file from location and store all XMLs to location from config file
print("unrar start")
filemanagement.unRarDownloadedFile( DOWNLOAD_LOCATION = config['DOWNLOAD_LOCATION']
                                    , DESTINATION_LOCATION = config['XML_LOCATION'] )
print("unrar complete")

xml_files_list = filemanagement.getFilesXMLFromOrigin( LOCATION = config['XML_LOCATION'] )
'''
print(xml_files_list)  # this is a list of dictionaries, each dictionary has file location and file name for all XMLs
'''
parserXML = ParserXML()
complete_sales_list = []
for XMLFile in xml_files_list:

    INPUT_PATH = XMLFile["INPUT_PATH"]
    FILE = XMLFile["FILE"]

    try:

        print("Parsing the file: " + FILE)

        current_sales_list = parserXML.parseXML( FILE, INPUT_PATH )
        complete_sales_list += current_sales_list
        #print(current_sales_list)

    except Exception as e:
        print("got exception when parsing XMLs")
        print(e)

#print(complete_sales_list)
offline_sales_external_df = pd.DataFrame(complete_sales_list)
print(offline_sales_external_df)

offline_sales_internal_df = pd.read_csv('csv/offline_internal_sales_data.csv')
print(offline_sales_internal_df.head())

# let's compare the columns of both the dataframes
print(offline_sales_external_df.columns)
print(offline_sales_internal_df.columns)

# it seems offline_sales_internal_df has an unwanted column called 'Unnamed: 0', lets drop it
offline_sales_internal_df.drop('Unnamed: 0', axis=1, inplace=True)
#print(offline_sales_internal_df.columns)

# offline_sales_internal_df has a name field which has to be split into firstname and lastname to match offline_sales_external_df
offline_sales_internal_df[['firstname', 'lastname']] = offline_sales_internal_df['name'].str.split(' ',expand=True)
# expand = True will insert None where there is only one part of the name, like first name only
#print(offline_sales_internal_df.head())
# now that we split name into newer columns, we can drop it
offline_sales_internal_df.drop('name', axis=1, inplace=True)
#print(offline_sales_internal_df.head())

# let's rename mailid to email_id so that column names match in both dataframes
offline_sales_internal_df.rename(columns={'mailid': 'email_id'}, inplace = True)
#print(offline_sales_internal_df.columns)

# we will create a column named 'value' which actually missing, its simply a result of unit_value * units
offline_sales_internal_df['value'] = offline_sales_internal_df.apply(lambda x: x['unit_value']*x['units'],
                                   axis=1)
#print(offline_sales_internal_df['value'])

# let's compare columns in both dataframes to see if any are missing
print(offline_sales_external_df.columns.difference(offline_sales_internal_df.columns).tolist())

# next we will gather online sales data from corresponding tables
# here we have to input name of the database on which input(sales) table resides
USER = config['USER']
PASS = config['PASSWORD']
HOST = config['HOST']
DATABASE = config['FROM_DATABASE']
PORT = '3306'

from sqlalchemy import create_engine
engine = create_engine('mysql+mysqlconnector://'+USER+':'+PASS+'@localhost:'+PORT+'/'+DATABASE, echo=False)

sql_query = """select * from online_sales """

online_sales_df = pd.read_sql(sql_query, engine)
print(online_sales_df)

# let's compare columns with previous dataframe to see if any are missing
print(offline_sales_external_df.columns.difference(online_sales_df.columns).tolist())

# we can rename purchase_value to value
online_sales_df.rename(columns={'purchase_value': 'value'}, inplace = True)
#print(offline_sales_external_df.columns.difference(online_sales_df.columns).tolist())

dataframe_list = [offline_sales_external_df, offline_sales_internal_df, online_sales_df]

consolidated_df = pd.concat(dataframe_list)
print(consolidated_df)
# it seems contact_no in offline_sales_internal_df has a hyphen that needs to be removed
#print(offline_sales_internal_df['contact_no'])
offline_sales_internal_df['contact_no'] = offline_sales_internal_df['contact_no'].apply(lambda x:x.replace('-', ''))
#print(offline_sales_internal_df['contact_no'])

# next, we write function to convert purchase_date in offline_sales_internal_df to  mm-dd-yyyy format
def date_formatting(date_input):
    date_list = date_input.split('/')
    #print(date_list)
    if len(date_list[0]) == 1:
        date_list[0] = '0'+date_list[0]
    return '-'.join(date_list)

offline_sales_internal_df['purchase_date'] = offline_sales_internal_df['purchase_date'].apply(date_formatting)

# we will also change the date format in online_sales_df
def date_reorder(date_input):
    date_list = date_input.strftime('%m-%d-%Y')
    return date_list

online_sales_df['purchase_date'] = online_sales_df['purchase_date'].apply(date_reorder)
#print(online_sales_df['purchase_date'])

consolidated_df = pd.concat(dataframe_list)
print(consolidated_df)

# we will perform further processing on the dataframe in the transform class
dataframe_transform = Transform(consolidated_df)
final_data = dataframe_transform.transform()

print(final_data)

final_update = FinalUpdate(final_data)
final_update.table_insert()

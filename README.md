# mutual fund ETL pipeline

Contains scripts that extract data from table and other sources, aggregates, transforms and loads into another database

Install packages from requirements.txt beforehand

create a .env file with following details
----------
USER=<MySQLUserID>
PASSWORD=<MySQLPassword>
HOST="localhost"
FROM_DATABASE=<fromDBname>
TO_DATABASE=<toDBname>
PORT="3306"

REQUEST_DOWNLOAD_URL="http://127.0.0.1:5000/download"
DOWNLOAD_LOCATION=<location-to-store-rar-files>
XML_LOCATION=<location-to-extract-rar-files-to>
------------

Make sure to copy file_service folder to a different location and run the host_file.py on a different virtual environment/interpreter than the one running main.py

Run input_table_data/table_insert.py to simulate a table from which we read entries

Run main.py only after previous steps are run 


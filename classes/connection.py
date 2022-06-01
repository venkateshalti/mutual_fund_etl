import mysql.connector # this package should be installed from pypi 'mysql-connector-python'
from mysql.connector import errorcode  # let's also import errorcode, this is a python module with all error names and codes for SQL connection fails
#this code sits in sitepackages inside mysql/connection folder
#print(type(errorcode))
#print(errorcode)
#print(type(errorcode.ER_ACCESS_DENIED_ERROR))

class Connection():  #class for establishing db connection

    def __init__(self, USER, PASS, HOST, DATABASE ):  # values needed for the connection are extracted by main from config

        self.USER = USER
        self.PASS = PASS
        self.HOST = HOST
        self.DATABASE = DATABASE

        try:  # exception handling by check if connection is successful
          cnx = mysql.connector.connect(user=self.USER  # calling connect method to create class instance of mysql connection
                                        , password=self.PASS
                                        , host=self.HOST
                                        , database=self.DATABASE)

          cnx.autocommit = False  # if we dont use this, rows will be written even before we call the .commit() method
          # once the code encounters conn.cursor().execute or conn.cursor().executemany, it will commit, thats why we have to set commit = False
          # if you want to commit, you have to call .commit() on the connection explicitly

          print("connected to dataBase")
          self.conn = cnx  # we renamed the connection,
          # but its instance retains as long as the parent main function that called the Connection() is not ended
          # we use this conn to write to database using cursor

        except mysql.connector.Error as err:  # we stored connection error exception in err

          if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:  # for access denial exception, print corresponding message
            print("Something is wrong with your user name or password")
          elif err.errno == errorcode.ER_BAD_DB_ERROR:  # bad database
            print("Database does not exist")
          else:  # for unpredicted exception, just print the message
            print(err)

    def execQuery(self, Query_params, params):  # actual code to write to database table
        cursor = self.conn.cursor()
        cursor.execute(Query_params, params)

    def commit(self):
        """ To commit inserts, you have to end with a commit """
        self.conn.commit()
class ExecuteSql():

    def __init__(self, connection):
        self.connection = connection # Connection class instance named connection is passed as argument

    def insert_records( self, sql_insert, params ):
        self.connection.execQuery(sql_insert, params)  # this connection is the object created in  connection.py
        # we created a execQuery method in connection to write to table via cursor, query and parameters
        self.connection.commit()  # don't forget to commit


"""
CREATE TABLE ctl_activity_process (
  etl_master varchar(50),
  id_process bigint(20),
  fichero varchar(100),
  descri_activity varchar(100),
  status int(11),
  start_date datetime,
  end_date datetime,
  cant_row int(11),
  fecha_desde date,
  fecha_hasta date
);

"""
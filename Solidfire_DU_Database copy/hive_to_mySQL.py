""""Connecting Hive To Pull Data Into Pandas Dataframe
This may take a second to run for the map reduce"""

import pyhs2
from bokeh.io import output_notebook, show, output_file
from credentials import *
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
# from mysql.connector import *
# import pandas as pd
# import numpy as np
# from bokeh.charts import Bar, Scatter
# from bokeh.charts import Histogram
# import bokeh as bokeh
from sqlalchemy import create_engine
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
output_notebook()


class Drivewearmodel:
	def __init__(self):
		host = "dma-tools-01.eng.solidfire.net"
		auth = "PLAIN"
		user = "ryan.riopelle"
		password = "Black12!"
		database = "aiq_prod"
		self.hive = pyhs2.connect(host=host,
								  port=10000,
								  authMechanism=auth,
								  user=user,
								  password=password,
								  database=database)

	def modeldata(self):
		rows = self._getRows()
		drivewearmodel_data = pd.DataFrame.from_records(rows, columns=['customer', 'cluster_uid', 'cluster_name',
																	   'cluster_version'])

		return drivewearmodel_data

	def _getRows(self):
		query = """
                    select distinct b.customer_name as customer, a.cluster_uid as cluster_uid,
                    c.cluster_name as cluster_name,  a.cluster_version as cluster_version
                    from
                      (
                    select *
                    from aiq_prod.cluster_versions
                    where ds = date_sub(current_date,1)
                     ) a
                     left outer join
                     (
                       select customer_uid,customer_name
                       from aiq_prod.customers
                       where ds = date_sub(current_date,1)
                     )b on a.customer_uid = b.customer_uid
                     left outer join
                     (
                       select cluster_uid, cluster_name
                       from aiq_prod.clusters
                       where ds =  date_sub(current_date,1)
						) c on a.cluster_uid = c.cluster_uid
        """
		cursor = self.hive.cursor()
		cursor.execute(query)
		rows = cursor.fetchall()
		cursor.close()
		return rows


# call class instance
i = Drivewearmodel()
# model data method creates panadas dataframe of the query
drivewearmodel_data = i.modeldata()

#sets up login information for the cursor
config = {
  'user': mySQLuser,
  'password': mySQLpassword,
  'host': mySQLhost,
  'database': mySQLdatabase,
  'raise_on_warnings': mySQLraise_on_warnings,
}

try:
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    print ("successfully connected and cursor created")
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with your user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist")
  else:
    print(err)
else:
  cnx.close()


try:
	engine = create_engine('mysql+mysqlconnector://solidfire:solidfire@dma-cpe-01/dma', echo=False)
	drivewearmodel_data.to_sql(name='hive_cluster_count_update', con=engine, if_exists='replace', index=False)
	print "Dataframe successfully imported"
except drivewearmodel_data.Error as err:
	if err == AttributeError:
		print "This is an attribute error"
	else:
		print err

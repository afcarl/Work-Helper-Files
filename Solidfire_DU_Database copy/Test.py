# import pip #needed to use the pip functions
# for i in pip.get_installed_distributions(local_only=True):
#     print(i)

import salesforce_reporting
from salesforce_reporting import Connection
from credentials import *
import pandas as pd
import numpy as np
import datetime
from datetime import datetime
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import logging
import unittest
import sqlalchemy
from sqlalchemy.types import NVARCHAR

def sqlcol(dfparam):

    dtypedict = {}
    for i,j in zip(dfparam.columns, dfparam.dtypes):
        if "object" in str(j):
            dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=255)})

        if "datetime" in str(j):
            dtypedict.update({i: sqlalchemy.types.DateTime()})

        if "float" in str(j):
            dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})

        if "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.INT()})

    return dtypedict



def build_sf_dict():
	# Connect to SalesForce using my credentials
	while True:
		try:
			sf = Connection(username=sfuser, password=sfpassword,
							security_token=sftoken)
			report = sf.get_report(sfreport)
			print "Connection to SalesForce was successful, report opened successfully."
			parser = salesforce_reporting.ReportParser(report)
			# Return a dictionary of the Open Engineering Escalations Report
			dictionary = parser.records_dict()
			return dictionary
			break
		except:
			print "Connect to SalesForce was unsuccessful, trying to connect again."
			break

# Pull Data In To Variable all_data


# def save_SF_mysql(sf_dictionary, table_name):
# 	while True:
# 		try:
sf_dictionary = build_sf_dict()


df = pd.DataFrame(sf_dictionary)
column_names = df.columns
no_spaces = []
for i in column_names:
    j = i.replace(' ', '')
    no_spaces.append(j)
df.columns = no_spaces

# convert each column to the mySQL format for date
df['DateReported'] = df['DateReported'].apply(
    lambda row: datetime.strptime(row, '%m/%d/%Y').strftime('%Y-%m-%d %H:%M:%S'))
df['DateResolved'] = df['DateResolved'].apply(
    lambda row: datetime.strptime(row, '%m/%d/%Y').strftime('%Y-%m-%d %H:%M:%S'))
df['DateReviewed'] = df['DateReviewed'].replace(['-'], ['1/1/1900']).apply(
    lambda row: datetime.strptime(row, '%m/%d/%Y').strftime('%Y-%m-%d %H:%M:%S')).replace(['1900-01-01 00:00:00'], [None])
df = df.replace(['-'], [None]).replace(['None'],[None]).replace(['None.'],[None])

config = {
  'user': mySQLuser,
  'password': mySQLpassword,
  'host': mySQLhost,
  'database': mySQLdatabase,
  'raise_on_warnings': mySQLraise_on_warnings
}
engine = create_engine('mysql+mysqlconnector://solidfire:solidfire@dma-cpe-01/dma')
df=df.astype(unicode)
col_types = sqlcol(df)
df.to_sql(name="TEST1", con=engine, if_exists='replace', index=False, chunksize=1, dtype = col_types)
print "done"
		# 	break
		# except:
		# 	print "Error saving salesforce to MySQL!!!"
		# 	break


            # Pull Data In To Variable all_data


# sf_dictionary = build_sf_dict()  # Save SF Dictionary
# save_SF_mysql(sf_dictionary, "sales_force_data")











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

	def sqlcol(self, dfparam):

		dtypedict = {}
		for i, j in zip(dfparam.columns, dfparam.dtypes):
			if "object" in str(j):
				dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=1000)})

			if "datetime" in str(j):
				dtypedict.update({i: sqlalchemy.types.DateTime()})

			if "float" in str(j):
				dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})

			if "int" in str(j):
				dtypedict.update({i: sqlalchemy.types.INT()})
		return dtypedict


	def transfer_data(self, query, table_name):

		rows, columns = self._getRows(query)
		drivewearmodel_data = pd.DataFrame.from_records(rows, columns=columns)

		print drivewearmodel_data
		try:
			engine = create_engine('mysql+mysqlconnector://solidfire:solidfire@dma-cpe-01/dma', echo=False)
			drivewearmodel_data = drivewearmodel_data.astype(unicode)
			col_type = self.sqlcol(drivewearmodel_data)
			print col_type
			return drivewearmodel_data.to_sql(name=table_name, con=engine, if_exists='replace', index=True, type=col_type)

			print "Data Transfered From Hive To SQL"
		except:
			print "Error with dataframe import to MySQL"
			print("Unexpected error:", sys.exc_info()[0])



	def _getRows(self, query):

		try:
			cursor = self.hive.cursor()
		except:
			print "Error getting data from Hive"

		cursor.execute(query)
		rows = cursor.fetchall()
		columns = self.get_column_names(cursor)
		cursor.close()
		return rows, columns

	def get_column_names(self, cursor):

		out = []
		for column in cursor.getSchema():
			out.append(column['columnName'])
		return out
#






































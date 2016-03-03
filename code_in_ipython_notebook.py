import pandas as pd
import xlrd
import numpy as np
import xlsxwriter

#import file from excel
df= pd.read_excel("ForIpython.xlsx" , sheetname=0, header=0)

#format to database structure
dfsum = pd.DataFrame()
for column in df:
    frame=df[column].reset_index()
    frame["id"]=column
    frame.columns=['intFY','curRequest','intProjectID']
    dfsum=dfsum.append(frame)
result = dfsum[np.isfinite(dfsum['cost'])]
print(result)
#1613 rows, 3 columns

#Open xls writer engine
writer = pd.ExcelWriter('pandas_simple.xlsx', engine='xlsxwriter')

# Convert the dataframe to an XlsxWriter Excel object.
result.to_excel(writer, sheet_name='Sheet1')

# Close the Pandas Excel writer and output the Excel file.
writer.save()thon
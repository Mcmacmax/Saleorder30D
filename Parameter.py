import pandas as pd
import numpy as np
import datetime
import pyodbc as db
import os
import glob
from dateutil.relativedelta import relativedelta

def writeB(df_outB):
    dfobj = pd.DataFrame(df_outB)
    df_write = dfobj.replace(np.nan,0)
    ##################################################### 1. Del Data and write from data frame
    #start_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #print ('DATE',start_datetime)
    """ database connection ={SQL Server Native Client 11.0};"""
    conn = db.connect('Driver={SQL Server Native Client 11.0};'
                        'Server=SBNDCBIPBST02;'
                        'Database=TSR_ADHOC;'
                        'Trusted_Connection=yes;')
    cursor = conn.cursor()
    ####sql_del ="delete FROM [TB_SR_Employee].[dbo].[TRACE_EMPLOYEE]"
    ####cursor.execute(sql_del)
    for index, row in df_write.iterrows():
        print(row)
        cursor.execute("""INSERT INTO TSR_ADHOC.dbo.Sale_Order_30D([Date],[Period_Date],[OWNER_ORDER],[LocationNameEN],[Region],[item],[Sum_QTY_CS],[Sum_QTY_CS_NTF]) 
        values(N'%s',N'%s',N'%s',N'%s',N'%s',N'%s','%f','%f')"""%\
            (row[0].strftime('%Y-%m-%d'),row[1],row[2],row[3],row[4],row[5],row[6],row[7])
        )   
    cursor.commit()
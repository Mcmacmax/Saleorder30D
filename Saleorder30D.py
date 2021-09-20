import pandas as pd
import numpy as np
import datetime
import pyodbc as db
import os
import glob
from dateutil.relativedelta import relativedelta
from Parameter import writeB as WB
from Log import log as log

start_datetime = datetime.datetime.now()
print (start_datetime,'execute')

today = datetime.datetime.now().strftime('%Y-%m-%d')

Date = datetime.datetime.now() - relativedelta(days=0)
date_1 = Date.strftime('%Y-%m-%d')
print(Date)
print(date_1)
#จุดเสีย่งจาก DATABASE
"""database connection ={SQL Server Native Client 11.0};"""
conn = db.connect('Driver={SQL Server Native Client 11.0};'
                    'Server=SBNDCBIPBST02;'	
                    'Database=TB_WarRoom;'
                    'Trusted_Connection=yes;')
cursor = conn.cursor()
dfout = pd.DataFrame(columns=['Date','Period_Date','OWNER_ORDER','LocationNameEN','Region','item','Sum_QTY_CS','Sum_QTY_CS_NTF'])
print('Connect DATABASE COMPLETED')
SQL = """ 
    declare @today date = '"""+str(date_1)+"""' 
    declare @dateFrom date = cast(dateadd(DAY,-30,@today) as date)
    ------------------------------ ThaiBev sale ----------------------------
    select A.Date
        ,A.Period_Date
        ,A.OWNER_ORDER
        ,B.LocationNameEN
        ,A.Region
        ,A.item 
        ,sum(A.SUM_QTY_CS) Sum_QTY_CS
        ,sum(A.SUM_QTY_INC_CREDIT) Sum_QTY_CS_NTF
        from(
    select 
    @today Date
    ,concat(@dateFrom,'_',@today) Period_Date
    ,OWNER_ORDER = 'THAIBEV'
    ,o.ActualSourceLocation AS WH_ID
    ,p.BIProduct_Label as item
    ,bl.Region
    ,case when O.ShipToText like N'%ON-ดี%' and p.BIProduct_Category in ('BEER','Spirits') then 'Y'
        when O.ShipToText like N'%ไป 6411สำนักงาน-ดี%' and p.BIProduct_Category in ('BEER','Spirits') then 'Y' 
        else 'N' end as covid_tranfer_flag
    ,sum(case when o.HandoverType in ('', 'X', ' ','Z','W') then o.TO_ReportQty else 0 end) as SUM_QTY_CS  -- X=สินค้าฝาก W=Backhaul Agent Z=ส่งตรง Sub-Agent
    ,sum(o.TO_ReportQty) as SUM_QTY_INC_CREDIT  -- to exclude order ขายเชื่อ during 3-6may2020 and เอาตัดฝากเข้ามาด้วย
    from [TB_WarRoom].[dbo].[TransportationOrder_TOMsSource] o
    left join [TB_WarRoom].[dbo].[D_Product_New] p ON o.[ITEM] = p.[Label]
    left join (select distinct Source_LocationLabel, BILocationLabel from [TB_WarRoom].[dbo].[D_LocationMapping]) lm on o.ActualSourceLocation = lm.Source_LocationLabel 
    left join [TB_WarRoom].[dbo].[D_BILocation] bl on lm.BILocationLabel = bl.BILabel

    where cast(O.CreatedDate AS DATE) >= @dateFrom 
    and [LogisticProvider] IN ( '7500' , 'NULL')
    and [ShipmentType] IN ('006', '007')
    and p.[FGFlag] = N'FG'
    and p.ProductCategory not in ('Other')
    and [TOStatusDesc] not in ('TBL Canceled', 'SO Canceled', 'Rejected', 'Closed')
    and case when substring(Ref_No,6,2) = 'TG' and Channel = 'TT' and p.BIProduct_Category in ('BEER','Spirits') then 'Y' --> exclude สินค้าฝาก หรือ order D
            else 'N' end = 'N'
        
    group by o.ActualSourceLocation
    ,p.BIProduct_Label
    ,bl.Region
    ,case when o.ShipToText like N'%ON-ดี%' and p.BIProduct_Category in ('BEER','Spirits') then 'Y'
        when o.ShipToText like N'%ไป 6411สำนักงาน-ดี%' and p.BIProduct_Category in ('BEER','Spirits') then 'Y' 
        else 'N' end 

    union
    ------------------------------ SSC sale ----------------------------
    select 
    @today Date
    ,concat(@dateFrom,'_',@today) Period_Date
    ,OWNER_ORDER = 'SSC'
    ,o.Plant_Code AS WH_ID
    ,p.BIProduct_Label AS ITEM
    ,bl.Region
    ,covid_tranfer_flag = 'N'
    ,sum(SaleQTY) AS SUM_QTY_CS
    ,sum(SaleQTY) as SUM_QTY_INC_CREDIT
    from [TB_WarRoom].[dbo].[SSC_ALLSALE_RawData_New] o
    left join [TB_WarRoom].[dbo].[D_Product_New] p ON o.Product_Code = p.[Label]
    left join (select distinct Source_LocationLabel, BILocationLabel from [TB_WarRoom].[dbo].[D_LocationMapping]) lm on o.Plant_Code = lm.Source_LocationLabel 
    left join [TB_WarRoom].[dbo].[D_BILocation] bl on lm.BILocationLabel = bl.BILabel
    where cast(SalesDate AS DATE) >= @dateFrom
    and p.[FGFlag] = N'FG'
    and p.ProductCategory not in ('Other')
    group by o.Plant_Code 
    ,p.BIProduct_Label
    ,bl.Region
    ) A
    left join (SELECT L.[Source_LocationLabel] ,BI.LocationNameEN
    FROM (select distinct [BILocationLabel],[Source_LocationLabel], [Source_LocationName], [FromSystem] from [TB_WarRoom].[dbo].[D_LocationMapping] where (FromSystem is null or FromSystem = 'SAP SSC')) L
    LEFT JOIN [TB_WarRoom].[dbo].[D_BILocation] BI ON L.[BILocationLabel] = BI.BILabel
    where [Source_LocationLabel] not in ('5711','6011','7811')) B on A.WH_ID=B.Source_LocationLabel
    group by
        A.Date
        ,A.Period_Date
        ,A.OWNER_ORDER
        ,B.LocationNameEN
        ,A.Region
        ,A.item 
""" 
cursor.commit()
#print(SQL)
cursor.execute(SQL)
print('QUERY_SQL')
data_Out = cursor.fetchall()
for row in data_Out:
    newrow= {'Date':row[0],'Period_Date':row[1],'OWNER_ORDER':row[2],'LocationNameEN':row[3],'Region':row[4],'item':row[5],'Sum_QTY_CS':row[6],'Sum_QTY_CS_NTF':row[7]}
    dfout = dfout.append(newrow, ignore_index=True)
data_In = dfout
print(data_In)

###################Write B to SQL ################
df_WB = WB(data_In)
print('Complte Write B to DATABASE')

end_datetime = datetime.datetime.now()
print ('---Start---',start_datetime)
print('---complete---',end_datetime)
DIFFTIME = end_datetime - start_datetime 
DIFFTIMEMIN = DIFFTIME.total_seconds()
print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')
log()
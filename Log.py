import pandas as pd
import numpy as np
import datetime
import pyodbc as db
import os
import glob
from dateutil.relativedelta import relativedelta

def log() :
    end_datetime = datetime.datetime.now()
    date = end_datetime.strftime('%Y-%m-%d')

    activitylog = 'Sucessfuly at '+str(end_datetime)+'****** \n'

    log_file = "Log_"+str(date)
    f=open(r'./log/'+log_file+'.txt',"a")
    f.write(activitylog)
    f.close()
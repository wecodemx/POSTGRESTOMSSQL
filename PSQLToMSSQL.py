import psycopg2
import pickle
import sys
import os
import time
import pyodbc
from signal import signal, SIGINT

MSSQLDB = "BD_AUTORRECUPERACION_LAVISTA"
MSSQLUSR = "AGUAPUEBLA\Administrator"
MSSQLPWD = ""
MSSQLTABLE = "dbo.mainsite_data"

CURR_ID = 0
RUNNING = True

def handler(signal_received, frame):
    global RUNNING
    RUNNING = False

signal(SIGINT, handler)

def saveId(id):
    try:
        file = open('CURR_ID', 'wb')
        pickle.dump(id, file)
        file.close()

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)

while RUNNING:
    try:
        file = open('CURR_ID', 'rb')
        CURR_ID = pickle.load(file)
        file.close()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)

    print(CURR_ID)

    try:
        mssql_conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};Server=SRV-IGNITION-1;Database="+MSSQLDB+";Trusted_Connection=yes;")
        #mssql_conn = pyodbc.connect("Driver={SQL Server Native Client 11};Server=localhost;Database="+MSSQLDB+"uid="+MSSQLUSR+";pwd="+MSSQLPWD)
        mssql_cursor = mssql_conn.cursor()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)

    try:
        conn = psycopg2.connect("dbname=atautomation user=atadmin password=automation host=10.8.0.1")
        cursor = conn.cursor()

        cursor.execute("select id,tag_id,timestamp,value from mainsite_data where id > "+str(CURR_ID)+" order by id asc")

        data = cursor.fetchall()

        if len(data) > 0:
            for d in data:
                if "float" not in str(type(d[3])):
                    d[3] = 0
                query = "insert into "+MSSQLTABLE+"(tag_id, timestamp,value) values ("+str(d[1])+", '"+str(d[2].strftime("%Y-%m-%d %H:%M:%S"))+"',"+str(d[3])+")"
                print(query)
                mssql_cursor.execute(query)

            mssql_conn.commit()

            last_id = data[len(data)-1][0]
            saveId(last_id)

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)

    time.sleep(60)

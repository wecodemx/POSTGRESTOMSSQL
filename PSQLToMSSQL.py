import psycopg2
import pickle
import sys
import os
import time
from signal import signal, SIGINT

MSSQLDB = "BD_AUTORECUPERACION_LAVISTA"
MSSQLUSR = "AGUAPUEBLA\Administrador"
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
        mssql_conn = pyodbc.connect("Driver={SQL Server Native Client 11};Server=localhost;Database="+MSSQLDB+";Trusted_Connection=yes;")
        #mssql_conn = pyodbc.connect("Driver={SQL Server Native Client 11};Server=localhost;Database="+MSSQLDB+"uid="+MSSQLUSR+";pwd="+MSSQLPWD)
        mssql_cursor = mssql_conn.cursor()
    except:
        print("Error while connecting to MSSQL")

    try:
        conn = psycopg2.connect("dbname=atautomation user=atadmin password=automation host=10.8.0.1")
        cursor = conn.cursor()

        cursor.execute("select id,tag_id,timestamp,value from mainsite_data where id > "+str(CURR_ID)+" order by id asc")

        data = cursor.fetchall()

        if len(data) > 0:
            for d in data:
                print("Insertando",d[1],d[2],d[3])
                mssql_cursor.execute("insert into "+MSSQLTABLE+"(tag_id, timestamp,value) values (?, ?, ?)", d[1],d[2],d[3])

            mssql_conn.commit()

            last_id = data[len(data)-1][0]
            saveId(last_id)

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)

    time.sleep(60)

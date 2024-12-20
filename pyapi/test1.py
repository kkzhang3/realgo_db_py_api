import datetime
import py_realdb_sdk as py_realdb_sdk

db = py_realdb_sdk.PYRealDB('127.0.0.1', 3001)

now = db.Ping()
print(now, now[1].strftime('%Y-%m-%d %H:%M:%S'))

tagconfigs = db.ReadTagConfig()
print(tagconfigs)

address = [0,1,2]
endtime = datetime.datetime.now()
startime = endtime - datetime.timedelta(minutes=180)
print('read data')
status, v = db.ReadData(address, startime, endtime)
#print(status, v)


import datetime
import py_realdb_sdk as py_realdb_sdk

db = py_realdb_sdk.PYRealDB('127.0.0.1', 3001)
address = [0,1,2]
endtime = datetime.datetime.now()
startime = endtime - datetime.timedelta(minutes=180)
print('read data')
status, v = db.ReadData(address, startime, endtime)
print(status, v)


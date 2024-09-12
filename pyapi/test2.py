import time
import datetime
import py_realdb_sdk as py_realdb_sdk
db = py_realdb_sdk.PYRealDB('127.0.0.1', 3001)


address = [10,11,12]

for k in range(0,10):
    print(f"k:{k}")
    status = db.WriteData(address, [311.2+k, 333.4+k, 344.5+k])    
    print(f"status: {status}")
    time.sleep(1)

endtime = datetime.datetime.now()
startime = endtime - datetime.timedelta(minutes=1)
status, v = db.ReadData(address, startime, endtime)
print(status, v)




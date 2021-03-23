# timer

Use a single list to add all timer entries
The primary key is the recieptHandle since it is unique to each receive message
The secondary key is the timeout in seconds. We don't record the timeout value instead we record the second when timeout happens
Use primary key (receiptHanle) to add/delete the timer 
The expiry processing uses secondary key to sort all entries and each second, process all the entries with secondary key (expire time in seconds) on or before current time in second

Any system support mutiple key index can be used to implement this timer system.

* **Implementation with GO's map**
This is implemented in timer.go
It uses multiple maps. Since both start/stop timer and expiry process are accessing all maps, lock is used to make sure the data integrity.
However there're still multiple issues:
- There are multiple operations in start/stop timer, since no transaction here, the start/stop is not atomic operation might cause data inconsistency.
- Pure memory operation, no persistent support. If crash happens, all timer data will lost, messages will be considered as delivered. 
- Since no persistent, obviously no mutiple site backup.

* **Implementation with buntDB (or other similar DB)**
This is implemented in timerdb.go
It creates a single table with recieptHandle as key.
A index is created based on expire time in seconds. Expiry process uses this index to find entries on or before current time.
Testig shows no significant difference between in memory or on disk DB.
Transaction helps to provide atomic operation.
- slower than GO map implementation
- with on disk DB, data can be recovered after process restart
- disk DB file can be sync to different site for redundant protection

* **Implementation with Redis (or other key/value pair storage)**
This is implemented in timerred.go 
It creates a single table with receiptHandle as key.
Added expire time in seconds into an index. Expiry process uses this index to find the entries.
- Redis transaction can be used to 

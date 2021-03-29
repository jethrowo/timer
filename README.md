# Timer

Use a single list to add all timer entries.
The primary key is the recieptHandle since it is unique to each receive message.
The secondary key is the timeout in seconds. We don't record the timeout value instead we record the second when timeout happens.
Use primary key (receiptHanle) to add/delete the timer.
The expiry processing uses secondary key to sort all entries. Each second it processes all the entries with secondary key (expire time in seconds) on or before current time.

Any system support multiple key index can be used to implement this timer system.

### **Implementation with GO's map**

This is implemented in timer.go
It uses multiple maps. Since both start/stop timer and expiry process are accessing all maps, lock is used for concurrent run of main routine and expiry process routine.
However there're still multiple issues:
  - There are multiple operations in start/stop timer, since no transaction here, the start/stop is not atomic operation might cause data inconsistency.
  - Pure memory operation, no persistent support. If crash happens, all timer data will lost, messages will be considered as delivered, means message lost could happen.
  - Since no persistent, obviously no multiple site backup for redundant.

### **Implementation with buntDB (or other similar DB)**

This is implemented in timerdb.go
It creates a single table with recieptHandle as key.
A index is created based on expire time in seconds. Expiry process uses this index to find entries on or before current time.
Testing shows no significant difference between in memory or on disk DB.
Transaction is used to provide atomic operation.
  - slower than GO map implementation
  - with on disk DB, data can be recovered after process restart/crash
  - disk DB file can be sync to different site to provide redundant protection
  - Since buntDB engine is lightweight, multiple instances can be deployed for scaling up

### **Implementation with Redis (or other key/value pair storage)**

This is implemented in timerred.go
It creates a single table with receiptHandle as key. And a secondary set using time in second as key. The content in the set is the list of receiptHandle.
Expiry process iterates through the set to find the entries and find the corresponding receiptHandle in the table. Concurrent goroutine is used in part of processing. This is due to the nature of Redis server. It handles huge concurrent request much better than handle large amount of sequence requests. 
Redis pipeline transaction is used to provide atomic operation. Redis client on go is concurrent safe.
  - Redis db can set to be persistent. Data can be restored after restart/crash.
  - Not using Redis key timeout feature, no need to register to timeout public event subject. No need to worry about lost event.
  - Slow compare to the other two. Use concurrent GOLAN routine to improve the performance. Redis is good at process large number of concurrent requests. Since timer program is a single client, limited by max connections per client. If there're are multiple timer processes, the performance could be improved.

# Performance
Performance testing created 1,000,000 timers. Each timer set a random expire second. Part of timer will be expired during the testing. The rest of timer will be canceled before testing finish. Data collected during the testing: total time used for creating all timers (avg to "µs per request"), total time used for cancel all timer, average each tick process time (each tick is one second, the processing time should not exceed 1 second, otherwise the timeout will not accurate. From table, all methods can easily achieve that).

| method | User time (s) | µs per request | comments |
| :---:|---:|---:|:---|
| GO map with lock | 2.7|add timer: 0.91|avg tick process: 3 ~ 4 ms |
|   ||del timer: 0.53|    |
|   |   |      |     |
| buntDB disk | 42 | add timer: 23 | avg tick process: 40 ms |
|    |  | del timer: 23 |   |
|    |   |   |    |
| buntDB memory | 37 | add timer: 17 | avg tick process: 40 ms|
|   |  | del timer: 19 |  |
|   |   |  |   |
| Redis with persistent disk dump | 130 | add timer: 50 | avg tick process: 8 ms, max: 116 ms |
|    |    | del timer: 42 | save using default strategy |
|   |    |   |   |
| Redis with persistent AOF | 130 | add timer: 56 | avg tick tick process: 8 ms, max: 400 ms |
|    |   | del timer: 41 | update AOF every second |
|   |   |   |  |
| Redis without persistent | 130 | add timer: 49 | avg tick process: 8 ms, max 346 ms |
|    |    | del timer: 40 |  |

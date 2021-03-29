package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"strconv"
	"sync"
	"time"
)

// use buntDB for timer
// with transaction, no lock is needed
type timerRedis struct {
	rdb   *redis.Client
	ctx   context.Context
	total int           // counter for all timer created
	delC  int           // counter for all timer canceled
	expC  int           // counter for expired timers
	avg   time.Duration // avg time used to process timer each tick (one second)
	max   time.Duration // max time used to process timer each tick
	min   time.Duration // min time used to process timer each tick
	nE    int           // number of cancel of expired timer
	lock  sync.Mutex    // lock used to protect counters during concurrent process
}

func (t *timerRedis) InitTimer() *timerRedis {
	t = &timerRedis{
		rdb:   nil,
		ctx:   context.Background(),
		total: 0,
		delC:  0,
		expC:  0,
		avg:   0,
		max:   0,
		min:   time.Duration(^uint64(0) >> 1),
		nE:    0}
	t.rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	if err := t.rdb.Ping(context.Background()).Err(); err != nil {
		fmt.Printf("can't connect to redis: %v\n", err)
	}

	return t
}

func (t *timerRedis) StartTimer(receiptHandle string, timeout int, metadata msgMeta) error {
	now := time.Now().Unix()
	setT := now + int64(timeout)
	metadata.Timeout = setT

	// use JSON string for any metadata saved together with each timer
	j, err := json.Marshal(metadata)
	if err != nil {
		fmt.Printf("Failed to encoding to JSON\n")
		return err
	}
	// use Transction Pipeline to provide atomic operation
	pipe := t.rdb.TxPipeline()
	pipe.Set(t.ctx, receiptHandle, string(j), 0)
	pipe.SAdd(t.ctx, strconv.FormatInt(setT, 10), receiptHandle)
	_, err = pipe.Exec(t.ctx)
	if err != nil {
		fmt.Printf("Failed to update database in start timer: %v\n", err)
	} else {
		t.total++
	}

	return err
}

func (t *timerRedis) StopTimer(receiptHandle string) error {
	r, err := t.rdb.Del(t.ctx, receiptHandle).Result()
	if err != nil {
		fmt.Printf("Failed to update database in stop timer: %v\n", err)
	} else if r == 0 {
		// These are timer already expired hence does not exist in Redis DB anymore
		t.nE++
	} else {
		t.delC++
	}

	return err
}

func (t *timerRedis) TickProcess() {
	// set initial value, this can be saved to a config and read each time it restart
	lastT := time.Now().Unix() - 5
	// run every seconds
	for {
		st := time.Now()
		now := time.Now().Unix()
		// start from last tick since if there's new timer added right after SMembers
		// call, it will be processed here
		var p = false // This is used for statistic counter only
		for i := lastT; i <= now; i++ {
			// Get all timer from set which will expire at this round of process
			results, err := t.rdb.SMembers(t.ctx, strconv.FormatInt(i, 10)).Result()
			if err != nil {
				fmt.Printf("Failed to get member for %d, %v\n", i, err)
			}
			if len(results) > 0 {
				p = true
			}
			for _, h := range results {
				// Use goroutine for each expired message processing
				// Sequence process in Redis is really slow
				go func(h string, i int64, t *timerRedis) {
					v, err := t.rdb.Get(t.ctx, h).Result()
					// we don't remove receipHandle from timer list
					// so the receiptHanle key might not there
					if v != "" {
						var msgD msgMeta
						err = json.Unmarshal([]byte(v), &msgD)
						// process the msgD, resend msg or put it into DLQ
					}
					pipe := t.rdb.TxPipeline()
					// Del from the message list
					pipe.Del(t.ctx, h).Err()
					// Remove from this time slot,
					// don't need to remove the timer slot, once the set is empty, Redis
					// will remove the key automatically
					pipe.SRem(t.ctx, strconv.FormatInt(i, 10), h)
					_, err = pipe.Exec(t.ctx)
					if err != nil {
						fmt.Printf("Failed to update database in expiry timer: %v\n", err)
					} else {
						t.lock.Lock()
						t.expC++
						t.lock.Unlock()
					}
				}(h, i, t)
			}
		}
		// Calculate the time used to process in this round
		ed := time.Now()
		if p {
			delta := ed.Sub(st)
			if delta < t.min {
				t.min = delta
			}
			if delta > t.max {
				t.max = delta
			}
			if t.avg == 0 {
				t.avg = delta
			} else {
				t.avg = (t.avg + delta) / 2
			}
		}

		lastT = now
		time.Sleep(1 * time.Second)
	}
}

func (t *timerRedis) PrintTimer() {
	fmt.Printf("Current time: %v\n", time.Now().Unix())
	var cur uint64
	var n int
	for {
		var keys []string
		var err error
		keys, cur, err = t.rdb.Scan(t.ctx, cur, "*", 10).Result()
		if err != nil {
			fmt.Printf("Failed to scan the db: %v\n", err)
			break
		}
		for _, k := range keys {
			t, err := t.rdb.Type(t.ctx, k).Result()
			if err != nil {
				fmt.Printf("Failed to get type for: %s, %v\n", k, err)
			} else {
				if t == "string" {
					fmt.Printf("Timer: %v still in db\n", k)
				} else if t == "set" {
					fmt.Printf("Timer tick: %v still in db\n", k)
				} else {
					fmt.Printf("Key: %v, type: %v\n", k, t)
				}
			}
		}
		n += len(keys)
		if cur == 0 {
			break
		}
	}
	fmt.Printf("Still total %d entries in db\n", n)
	fmt.Printf("Total created: %v, expired: %v, canceled: %v, not-exit: %v\n", t.total, t.expC, t.delC, t.nE)
	fmt.Printf("Average tick process time, avg: %v, min: %v, max: %v\n", t.avg, t.min, t.max)
}

func (t *timerRedis) CloseTimer() {
}

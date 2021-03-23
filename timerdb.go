package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tidwall/buntdb"
	"time"
)

// use buntDB for timer
// with transaction, no lock is needed
type timerDB struct {
	db    *buntdb.DB
	total int
	delC  int
	expC  int
}

func (t *timerDB) InitTimer() *timerDB {
	var err error
	t = &timerDB{nil, 0, 0, 0}
	t.db, err = buntdb.Open("data.db")
	t.db.CreateIndex("timer", "*", buntdb.IndexJSON("Timeout"))
	if err != nil {
		fmt.Printf("%v\n", err)
	}

	return t
}

func (t *timerDB) StartTimer(receiptHandle string, timeout int, metadata msgMeta) error {
	now := time.Now().Unix()
	setT := now + int64(timeout)
	metadata.Timeout = setT

	j, err := json.Marshal(metadata)
	if err != nil {
		fmt.Printf("Failed to encoding to JSON\n")
		return err
	}
	// fmt.Printf("set timer %s to %s\n", receiptHandle, string(j))
	err = t.db.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(receiptHandle, string(j), nil)
		return err
	})
	if err != nil {
		fmt.Printf("Failed to update database in start timer: %v\n", err)
	} else {
		t.total++
	}

	return err
}

func (t *timerDB) StopTimer(receiptHandle string) error {
	err := t.db.Update(func(tx *buntdb.Tx) error {
		_, err := tx.Delete(receiptHandle)
		return err
	})
	if err != nil {
		if !errors.Is(err, buntdb.ErrNotFound) {
			fmt.Printf("Failed to update database in stop timer: %v\n", err)
		}
	} else {
		t.delC++
	}

	return err
}

func (t *timerDB) TickProcess() {
	// run every seconds
	for {
		now := time.Now().Unix() + 1
		delTo := fmt.Sprintf(`{"Timeout":%d}`, now)
		var delkeys []string
		t.db.View(func(tx *buntdb.Tx) error {
			tx.AscendLessThan("timer", delTo, func(key, value string) bool {
				var data msgMeta
				if err := json.Unmarshal([]byte(value), &data); err != nil {
					fmt.Printf("json decoding failed: %v\n", err)
				} else {
					// process message resend/handle dlq, etc.
				}

				delkeys = append(delkeys, key)
				// fmt.Printf("expire timer: %s - %v\n", key, value)
				return true
			})
			return nil
		})

		t.db.Update(func(tx *buntdb.Tx) error {
			var err error
			for _, k := range delkeys {
				if _, err = tx.Delete(k); err != nil {
					fmt.Printf("Failed to delete key: %v, %v\n", k, err)
					break
				} else {
					t.expC++
				}
			}
			return err
		})

		time.Sleep(1 * time.Second)
	}
}

func (t *timerDB) PrintTimer() {
	fmt.Printf("Current time: %v\n", time.Now().Unix())
	t.db.View(func(tx *buntdb.Tx) error {
		tx.AscendKeys("*", func(k, v string) bool {
			fmt.Printf("timer: %v - %v\n", k, v)
			return true
		})
		return nil
	})
	fmt.Printf("Total created: %v, expired: %v, canceled: %v\n", t.total, t.expC, t.delC)
}

func (t *timerDB) CloseTimer() {
	t.db.Close()
}

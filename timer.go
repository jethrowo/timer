package main

import (
	"fmt"
	"sync"
	"time"
)

type msgMeta struct {
	Dlq      string
	QURL     string
	Relcount int
	Timeout  int64
}

type void struct{}

type handleList map[string]void

// use the pure GO map for timer
// has to use lock to prevent concurrent operation of maps
type timer struct {
	msgQueue  map[string]msgMeta
	timeQueue map[int64]handleList
	lock      sync.Mutex
	total     int
	delC      int
	expC      int
	avg       time.Duration
}

func (t *timer) InitTimer() timert {
	t = &timer{}
	t.msgQueue = make(map[string]msgMeta)
	t.timeQueue = make(map[int64]handleList)
	t.total = 0
	t.delC = 0
	t.expC = 0
	t.avg = 0

	return t
}

func (t *timer) StartTimer(receiptHandle string, timeout int, metadata msgMeta) error {
	now := time.Now().Unix()
	setT := now + int64(timeout)
	metadata.Timeout = setT
	t.lock.Lock()
	t.msgQueue[receiptHandle] = metadata
	h, ok := t.timeQueue[setT]
	if ok {
		h[receiptHandle] = void{}
	} else {
		h = make(map[string]void)
		h[receiptHandle] = void{}
	}
	t.timeQueue[setT] = h
	t.total++
	t.lock.Unlock()

	// fmt.Printf("Add timer: %v at %v(%v)\n", receiptHandle, timeout, setT)
	return nil
}

func (t *timer) StopTimer(receiptHandle string) error {
	t.lock.Lock()
	m, ok := t.msgQueue[receiptHandle]
	if ok {
		ti := m.Timeout
		delete(t.msgQueue, receiptHandle)
		h, e := t.timeQueue[ti]
		if e {
			delete(h, receiptHandle)
		}
		if len(h) == 0 {
			delete(t.timeQueue, ti)
		}
		t.delC++
	}
	t.lock.Unlock()

	// fmt.Printf("Stop timer: %v\n", receiptHandle)
	return nil
}

func (t *timer) TickProcess() {
	// run every seconds
	lastT := time.Now().Unix() - 5
	var n time.Duration = 0
	for {
		st := time.Now()
		now := time.Now().Unix()
		for i := lastT; i <= now; i++ {
			t.lock.Lock()
			h, e := t.timeQueue[i]
			if e {
				for key, _ := range h {
					m := t.msgQueue[key]
					if m.Relcount > 0 {
					}
					// fmt.Printf("%v timeout at: %v\n", key, now)
					// check m.relcount
					// if count > 0, read msg, put it back to original queue
					// if count == 0, read msg, put it into dlq
					delete(t.msgQueue, key)
					t.expC++
				}
				delete(t.timeQueue, i)
			}
			t.lock.Unlock()
		}
		n += 1
		delta := time.Since(st)
		t.avg = (delta-t.avg)/n + t.avg

		lastT = now + 1

		time.Sleep(1 * time.Second)
	}
}

func (t *timer) PrintTimer() {
	fmt.Printf("Current time: %v\n", time.Now().Unix())
	for k, v := range t.timeQueue {
		fmt.Printf("timer: %v", k)
		for h, _ := range v {
			fmt.Printf(" for handler: %v", h)
		}
		fmt.Printf("\n")
	}
	fmt.Printf("Total created: %v, expired: %v, canceled: %v\n", t.total, t.expC, t.delC)
	fmt.Printf("Average tick process time: %v\n", t.avg)
}

func (t *timer) CloseTimer() {
}

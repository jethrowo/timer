package main

import (
	"encoding/base64"
	"fmt"
	"math/rand"
	"time"
)

type sampleData struct {
	h string
	t int
}

const sample = 1000000

func tryoutTimer(timert interface{}) {
	// generate sample data for testing. receiptHandle is base64 encoding
	// timeout value is random value between 1~21
	var s [sample]sampleData
	rand.Seed(time.Now().UnixNano())
	for j := 0; j < sample; j++ {
		sample_data := rand.Intn(20) + 1
		s[j].h = base64.StdEncoding.EncodeToString([]byte(time.Now().String()))
		s[j].t = sample_data
	}

	// init timer, start tick process
	/*
		ti, ok := timert.(*timer)
		if !ok {
			fmt.Printf("Wrong data type!\n")
			return
		}
	*/
	ti, ok := timert.(*timerDB)
	if !ok {
		fmt.Printf("Wrong data type!\n")
		return
	}
	/*
		ti, ok := timert.(*timerRedis)
		if !ok {
			fmt.Printf("Wrong data type!\n")
			return
		}
	*/

	ti = ti.InitTimer()
	go ti.TickProcess()
	// use following as sample message data
	mm := msgMeta{"dlq", "myqueue", 5, 0}
	// start all sample timer
	cur := time.Now()
	for i := 0; i < sample; i++ {
		// var s string
		// s = fmt.sprintf("abc%d", i)
		ti.StartTimer(s[i].h, s[i].t, mm)
	}
	end := time.Now()
	fmt.Printf("Create %d timer used %v\n", sample, end.Sub(cur))
	// wait a while for some timer to expire
	time.Sleep(10 * time.Second)
	// cancel all rest timer
	cur = time.Now()
	for i := 0; i < sample; i++ {
		ti.StopTimer(s[i].h)
	}
	end = time.Now()
	fmt.Printf("Cancel %d timer used %v\n", sample, end.Sub(cur))
	// wait a while for some timer to expire
	// print any timer still there and statistics
	ti.PrintTimer()
	// wait a while to see what happens for those canceled timers
	time.Sleep(11 * time.Second)
	fmt.Printf("Now if there's still timer:\n")
	ti.PrintTimer()
	ti.CloseTimer()
}

func main() {
	var t *timerDB
	// var t *timer
	// var t *timerRedis

	tryoutTimer(t)
}

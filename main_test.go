package main

import (
	// "github.com/stretchr/testify/assert"
	"encoding/base64"
	// "fmt"
	"math/rand"
	"testing"
	"time"
)

func teststartstoptimer(t *testing.t) {
	// a := assert.new(t)
	var s [1000000]sampledata
	rand.seed(time.now().unixnano())

	for j := 0; j < 1000000; j++ {
		sample_data := rand.intn(20) + 1
		s[j].h = base64.stdencoding.encodetostring([]byte(time.now().string()))
		s[j].t = sample_data
	}
	ti := inittimer()
	go ti.tickprocess()
	mm := msgmeta{"dlq", "myqueue", 5, 0}
	for i := 0; i < 1000000; i++ {
		// var s string
		// s = fmt.sprintf("abc%d", i)
		ti.starttimer(s[i].h, s[i].t, mm)
	}
	time.sleep(7 * time.second)
	for i := 0; i < 1000000; i++ {
		ti.stoptimer(s[i].h)
	}
	ti.printtimer()
}

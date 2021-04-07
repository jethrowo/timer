package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type startEvent struct {
	ID       timerID
	Timeout  time.Time
	Metadata msgMeta
}
type persistEvent struct {
	Start  *startEvent `json:",omitempty"`
	Stop   timerID     `json:",omitempty"`
	Expire timerID     `json:",omitempty"`

	Committed chan<- struct{} `json:"-"`
}

// Note: changing MaxHours to anything other than 12 or 24 will require some code changes
// specifically around indexing with `t.cur.Hour()`
const MaxHours = 12

type timerID string
type timerwheel struct {
	Persist chan<- persistEvent
	ctx     context.Context
	cancel  func()

	expC, startC, stopC uint64

	processTime []time.Duration

	lock sync.RWMutex
	th   [MaxHours][]timerID
	tm   [60][]timerID
	ts   [60][]timerID
	t    map[timerID]msgMeta
	cur  time.Time
}

func KafkaPersist(ctx context.Context, kafkaAddr net.Addr, kafkaTopic string) chan<- persistEvent {
	conn, err := kafka.Dial("tcp", kafkaAddr.String())
	if err != nil {
		panic(fmt.Errorf("kafka dial: %w", err))
	}
	ctlr, err := conn.Controller()
	if err != nil {
		panic(fmt.Errorf("kafka controller: %w", err))
	}
	conn.Close()
	conn, err = kafka.Dial("tcp", fmt.Sprintf("%s:%d", ctlr.Host, ctlr.Port))
	if err != nil {
		panic(fmt.Errorf("kafka dial controller: %w", err))
	}
	defer conn.Close()
	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             kafkaTopic,
		NumPartitions:     1,
		ReplicationFactor: -1,
	})
	if err != nil {
		panic(fmt.Errorf("kafka create topic: %w", err))
	}
	for {
		parts, err := conn.ReadPartitions(kafkaTopic)
		if err != nil {
			panic(fmt.Errorf("read partitions: %w", err))
		}
		if len(parts) > 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}

	fmt.Printf("persisting to kafka (%s %s)...\n", kafkaAddr, kafkaTopic)

	ret := make(chan persistEvent)
	kmsgs := make(chan []kafka.Message)
	go func() {
		msgs := make([]kafka.Message, 0, 10)
		safe := false
		pending := make([]chan<- struct{}, 0)
		for {
			select {
			case <-ctx.Done():
				return
			case kmsgs <- msgs:
				msgs = msgs[:0]
				for _, p := range pending {
					if p != nil {
						close(p)
					}
				}
				pending = pending[:0]
			case ev := <-ret:
				val, err := json.Marshal(ev)
				if err != nil {
					panic(err)
				}
				if safe {
					pending = append(pending, ev.Committed)
				} else {
					close(ev.Committed)
				}
				msgs = append(msgs, kafka.Message{Value: val})
			}
		}
	}()
	go func() {
		writer := kafka.Writer{
			Addr:  kafkaAddr,
			Topic: kafkaTopic,
		}
		for {
			select {
			case <-ctx.Done():
				return
			case msgs := <-kmsgs:
				if len(msgs) == 0 {
					time.Sleep(500 * time.Millisecond)
				}

				err := writer.WriteMessages(ctx, msgs...)
				if err != nil {
					panic(err)
				}
			}
		}
	}()
	return ret
}

func (t *timerwheel) InitTimer() timert {
	t = &timerwheel{
		cur:         time.Now(),
		t:           make(map[timerID]msgMeta),
		processTime: make([]time.Duration, 0),
	}
	t.ctx, t.cancel = context.WithCancel(context.Background())
	for i := 0; i < 60; i++ {
		t.ts[i] = make([]timerID, 0)
	}
	for i := 0; i < 60; i++ {
		t.tm[i] = make([]timerID, 0)
	}
	for i := 0; i < MaxHours; i++ {
		t.th[i] = make([]timerID, 0)
	}
	t.Persist = KafkaPersist(t.ctx, kafka.TCP("kafka:9092"), "perf-"+time.Now().Format("20060102150405"))

	return t
}

func (t *timerwheel) StartTimer(receiptHandle string, timeout0 int, metadata msgMeta) error {
	deadline := time.Now().Add(time.Duration(timeout0) * time.Second)
	tid := timerID(receiptHandle)
	metadata.Timeout = deadline.Unix()
	t.lock.Lock()
	timeout := deadline.Sub(t.cur)
	if timeout < 0 {
		t.expC++
		return nil
	} else if timeout > MaxHours*time.Hour {
		return errors.New("timeout must be <12hrs")
	}

	if timeout > 1*time.Hour {
		ih := timeout / time.Hour
		t.th[ih] = append(t.th[ih], tid)
	} else if timeout > 1*time.Minute {
		im := timeout / time.Minute
		t.tm[im] = append(t.tm[im], tid)
	} else {
		is := timeout / time.Second
		t.ts[is] = append(t.ts[is], tid)
	}
	t.t[tid] = metadata
	t.startC++
	t.lock.Unlock()
	persist := t.Persist
	if persist != nil {
		ctx := t.ctx
		committed := make(chan struct{})
		persist <- persistEvent{
			Start: &startEvent{
				ID:       tid,
				Timeout:  deadline,
				Metadata: metadata,
			},
			Committed: committed,
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-committed:
		}
	}
	return nil
}

func (t *timerwheel) StopTimer(receiptHandle string) error {
	found := false
	t.lock.Lock()
	tid := timerID(receiptHandle)
	if _, found = t.t[tid]; found {
		t.stopC++
		delete(t.t, tid)
	}
	t.lock.Unlock()
	if found {
		// copy stuff we want to use without a lock
		persist := t.Persist
		if persist != nil {
			ctx := t.ctx
			committed := make(chan struct{})
			persist <- persistEvent{Stop: tid, Committed: committed}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-committed:
			}
		}
	}
	return nil
}

func (t *timerwheel) TickProcess() {
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-time.After(1 * time.Second):
		}
		t.lock.RLock()
		delta := time.Since(t.cur)
		if delta < time.Second {
			t.lock.RUnlock()
			continue
		}
		t.lock.RUnlock()
		t.lock.Lock()
		for ; delta > time.Second; delta -= time.Second {
			pstart := time.Now()

			for _, tid := range t.ts[t.cur.Second()] {
				if _, found := t.t[tid]; found {
					t.expC++
					delete(t.t, tid)
					persist := t.Persist
					if persist != nil {
						ctx := t.ctx
						committed := make(chan struct{})
						persist <- persistEvent{Expire: tid, Committed: committed}
						select {
						case <-ctx.Done():
							return
						case <-committed:
						}
					}
				}
			}
			t.ts[t.cur.Second()] = t.ts[t.cur.Second()][:0]

			t.cur.Add(1 * time.Second)
			if t.cur.Second() == 0 {
				im := t.cur.Minute()
				for _, tid := range t.tm[im] {
					if meta, found := t.t[tid]; found {
						js := t.cur.Sub(time.Unix(meta.Timeout, 0)) / time.Second
						t.ts[js] = append(t.ts[js], tid)
					}
				}
				t.tm[im] = t.tm[im][:0]

				if im == 0 {
					ih := t.cur.Hour() % 12
					for _, tid := range t.th[ih] {
						if meta, found := t.t[tid]; found {
							jm := t.cur.Sub(time.Unix(meta.Timeout, 0)) / time.Minute
							t.tm[jm] = append(t.tm[jm], tid)
						}
					}
					t.th[ih] = t.th[ih][:0]
				}
			}
			pend := time.Now()
			t.processTime = append(t.processTime, pend.Sub(pstart))
			t.lock.Unlock()
			t.lock.Lock()
		}
		t.lock.Unlock()
	}
}

func (t *timerwheel) PrintTimer() {
	t.lock.RLock()
	fmt.Printf("Current time: %d (tick: %d)\n", time.Now().Unix(), t.cur.Unix())
	for tid, meta := range t.t {
		fmt.Printf("timer: %s, deadline: %s\n", tid, time.Unix(meta.Timeout, 0))
	}
	fmt.Printf("Total created: %d, expired: %d, canceled: %d\n", t.startC, t.expC, t.stopC)
	if len(t.processTime) > 0 {
		avg := float64(0)
		n := float64(len(t.processTime))
		min := time.Duration((1 << 63) - 1)
		max := time.Duration(0)
		for _, pt := range t.processTime {
			avg += float64(pt) / n
			if pt > max {
				max = pt
			}
			if pt < min {
				min = pt
			}
		}
		fmt.Printf("Average tick process time: %v (n: %d, min: %v, max: %v)\n", avg, len(t.processTime), min, max)
	} else {
		fmt.Printf("Average tick process time: <didn't process anything?>")
	}
	t.lock.RUnlock()
}

func (t *timerwheel) CloseTimer() {
	cancel, persist := t.cancel, t.Persist
	if cancel != nil {
		cancel()
	}
	if persist != nil {
		close(persist)
	}
}

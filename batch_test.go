package go_batcher

import (
	"github.com/stretchr/testify/require"
	"log"
	"sync"
	"testing"
	"time"
)

type testInstance[T any] struct {
	wg        *sync.WaitGroup
	tick      chan time.Time
	wait      chan struct{}
	b         *Batcher[T]
	t         *testing.T
	expectErr error
	expect    []T
}
type action[T any] func(*testInstance[T])
type testCase[T any] struct {
	name    string
	actions []action[T]
}

func TestTransfer(t *testing.T) {
	tests := []testCase[int]{
		{
			name: "lots in one",
			actions: []action[int]{
				request[int](100000, 3),
				resize[int](1000000),
				process[int](1),
			},
		},
		{
			name: "split second",
			actions: []action[int]{
				request[int](7, 7),
				resize[int](5),
				process[int](10),
			},
		},
		{
			name: "small batch lots of times",
			actions: []action[int]{
				request[int](30, 3),
				resize[int](2),
				process[int](45),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, test(tt.actions))
	}
}

func test[T any](actions []action[T]) func(*testing.T) {
	return func(t *testing.T) {
		i := &testInstance[T]{
			tick: make(chan time.Time),
			wait: make(chan struct{}),
			wg:   &sync.WaitGroup{},
			t:    t,
		}
		i.b = New(0, &time.Ticker{C: i.tick}, execute(i))
		go i.b.Run()
		defer close(i.tick)
		defer close(i.wait)
		start := time.Now()
		for _, a := range actions {
			a(i)
		}
		i.wg.Wait()
		log.Println("took", time.Since(start))
	}
}
func execute[T any](t *testInstance[T]) Fn[T] {
	return func(ts []T) error {
		t.expect = nil
		go func() {
			for t.b.pending != nil { // Wait for requests to execute
				time.Sleep(time.Millisecond)
			}
			t.wait <- struct{}{}
		}()
		return t.expectErr
	}
}
func request[T any](n, m int) action[T] {
	return func(t *testInstance[T]) {
		t.wg.Add(n)
		before := t.b.ctr
		for j := 0; j < n; j++ {
			go func() {
				defer t.wg.Done()
				err := <-t.b.Batch(make([]T, m))
				require.Equal(t.t, t.expectErr, err)
			}()
		}
		for t.b.ctr != before+n*m { // Wait for requests to be added
			time.Sleep(time.Millisecond)
		}
	}
}
func process[T any](n int) action[T] {
	return func(t *testInstance[T]) {
		for i := 0; i < n; i++ {
			size := t.b.ctr
			if t.b.size < size {
				size = t.b.size
			}
			t.expect = append(t.expect, make([]T, size)...)
			t.expectErr = nil     // TODO
			t.tick <- time.Time{} // Tick batcher
			<-t.wait              // Wait execute
		}
	}
}
func resize[T any](size int) action[T] { return func(i *testInstance[T]) { i.b.resize <- size } }

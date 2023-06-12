package go_batcher

import (
	"errors"
	"fmt"
	"time"
)

var (
	DefaultSize   = 10
	DefaultPeriod = time.Second / 2
)

// Pending is a container for pending responses
type pending struct {
	index uint32
	ch    ErrCh
}

// Batch is a container for a batch request
type Batch[T any] struct {
	ch    chan error
	items []T
}

// Fn is a type for the function to run
type Fn[T any] func([]T) error

// Batcher is the type that does all the work
type Batcher[T any] struct {
	size, ctr int
	tick      *time.Ticker
	fn        Fn[T]

	skip    bool
	carry   Errs
	pending []pending
	queue   []Batch[T]
	in      chan Batch[T]
	resize  chan int
}

// New creates a new *Batcher[T]
// size defaults to 10
// t defaults to a new *time.Ticker with DefaultPeriod
func New[T any](size int, t *time.Ticker, fn Fn[T]) *Batcher[T] {
	if size == 0 {
		size = DefaultSize
	}
	if t == nil {
		t = time.NewTicker(DefaultPeriod)
	}
	return &Batcher[T]{size: size, tick: t, fn: fn,
		in: make(chan Batch[T]), resize: make(chan int)}
}

// SetPeriod changes pace at which the function is called
func (b *Batcher[T]) SetPeriod(d time.Duration) { b.tick.Reset(d) }

// SetSize new batches will have this size, writing will be disabled if size is set to 0
func (b *Batcher[T]) SetSize(i int) { b.resize <- i }

// Close closes the input chanel but doesn't stop until it's empty
func (b *Batcher[T]) Close() {
	close(b.in) // Closes input
	<-b.resize  // Waits for stop
}

func (b *Batcher[T]) Batch(t []T) <-chan error {
	ch := make(chan error)
	b.in <- Batch[T]{ch, t}
	return ch
}

func (b *Batcher[T]) Run() {
	defer func() {
		if err := recover(); err != nil {
			b.Run() // Recover until closed
			return  // Don't close resize until closed
		}
		close(b.resize) // Close at the end
	}()
	var open, closed bool
	var batch Batch[T]
	var size int
	for {
		select {
		case size = <-b.resize: // Resize
			if size >= 0 { // Only allow positive numbers, or zero to disable writing
				b.size = size
			}
		case batch, open = <-b.in: // Enqueue
			if !open { // On chanel close
				closed = true // Close
				continue      // Drain
			}
			b.ctr += len(batch.items)        // Increment counter
			b.queue = append(b.queue, batch) // Push request to queue
		case _, open = <-b.tick.C: // Dequeue
			if !open { // If closed
				return // Stop
			} else if b.size == 0 || b.pending != nil { // If disabled or processing
				continue // Skip
			} else if b.ctr == 0 { // If empty
				if closed { // And closed
					return // Stop
				}
				continue // Skip
			}
			go b.exec(b.pop()) // Allow writing while reading (pop is sync, exec is async)
		}
	}
}

func (b *Batcher[T]) pop() (batch []T) {
	var length, cursor int
	var request Batch[T]
	if b.ctr > b.size { // requests exceed batch size
		batch = make([]T, b.size)
		left := b.size
		var i int
		for i, request = range b.queue {
			if b.skip { // Skip if request already crashed
				b.skip = false
				continue
			}
			if length = len(request.items); length > left { // If request doesn't fit in current batch
				b.pending = append(b.pending, pending{index: uint32(cursor)}) // Add to pending
				copy(batch[cursor:cursor+left], request.items[:left])         // Copy items that fit
				b.queue[i].items = request.items[left:]                       // Remove them from request
				break
			}
			b.pending = append(b.pending, pending{index: uint32(cursor), ch: request.ch}) // Add to pending
			copy(batch[cursor:cursor+length], request.items)                              // Copy items in full
			cursor += length                                                              // Move cursor
			left -= length                                                                // Remove space consumed
		}
		b.queue = b.queue[i:] // Pop items from queue
		b.ctr -= b.size       // Remove batch size
	} else { // all requests fit in batch
		batch = make([]T, b.ctr) // Batch for queued items
		for _, request = range b.queue {
			if b.skip { // Skip if request already crashed
				b.skip = false
				continue
			}
			length = len(request.items)                                                   // Request size
			b.pending = append(b.pending, pending{index: uint32(cursor), ch: request.ch}) // Add to pending
			copy(batch[cursor:cursor+length], request.items)                              // Copy items in full
			cursor += length                                                              // Move cursor
		}
		b.queue = nil // Clean queue
		b.ctr = 0     // Clean counter
	}
	return batch
}

func (b *Batcher[T]) exec(batch []T) {
	var err error
	defer func() { // Make sure some response happens
		var index int
		if e := recover(); e != nil {
			err = errors.New(fmt.Sprint(e))
		}
		errs, ok := err.(Errs)
		if err != nil && !ok { // if crash remove batch from queue and return the error
			for _, p := range b.pending {
				if p.ch == nil { // If partial
					p.ch, b.skip = b.queue[0].ch, true // skip rest of the batch and fail
				}
				p.ch.Res(err) // Respond to batch requests
			}
			b.pending = nil // Clean pending
			b.carry = nil   // Clean carry
			return
		}
		for i, p := range b.pending {
			if p.ch == nil { // If (last) batch is split
				b.carry = append(b.carry, errs...) // Add to carry
				break
			}
			var res Errs // Partial or no error
			if len(b.pending) == i+1 {
				index = len(errs) // If none found take all errors
				for j := range errs {
					if errs[j].Index > p.index { // Assumes ordered errors
						index = i
						break
					}
				}
				if index > 0 { // If found any error
					res, errs = errs[:index], errs[index:] // Pop errors
				}
			}
			if b.carry != nil { // If has carry
				res = append(b.carry, res...) // Prepend carry to response
				b.carry = nil
			}
			p.ch.Res(res) // Respond
		}
		b.pending = nil // Clean pending
	}()
	err = b.fn(batch)
}

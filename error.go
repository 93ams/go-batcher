package go_batcher

import (
	"strconv"
	"unsafe"
)

type Errs []struct {
	Index  uint32
	Result uint32
}

func (e Errs) Error() string { return strconv.Itoa(len(e)) }
func MultiErr[T any](t []T) error {
	if len(t) == 0 {
		return nil
	}
	return *(*Errs)(unsafe.Pointer(&t))
}
func MultiErr2[T any](t []T, err error) error {
	if err != nil {
		return err
	}
	return MultiErr(t)
}

type ErrCh chan<- error

func (c ErrCh) Res(err error) {
	defer close(c)
	if err == nil || err.Error() == "0" {
		return
	}
	select {
	case c <- err:
	default:
	}
}

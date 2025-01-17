//go:build integration
// +build integration

package tptest

import (
	"sync"
	"sync/atomic"

	"gitlab.corp.my.games/adventures/go/tp.git"
)

type counter struct {
	tp.DisableMarshaler
	sync.WaitGroup
	cnt int32
}

func newCounter(n int) *counter {
	ret := &counter{}

	ret.Add(n)

	return ret
}

func (cnt *counter) Inc() {
	atomic.AddInt32(&cnt.cnt, 1)
}

func (cnt *counter) Dec() {
	atomic.AddInt32(&cnt.cnt, -1)
}

func (cnt *counter) Get() int32 {
	return atomic.LoadInt32(&cnt.cnt)
}

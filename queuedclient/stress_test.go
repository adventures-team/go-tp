//go:build stress
// +build stress

package queuedclient_test

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

const (
	goroutines = 10
	idsCount   = 100
)

func TestIProtoStress(t *testing.T) {
	var prev, current uint64
	client := getTestClient(t)

	ids := make([]uint64, idsCount)
	for i := 0; i < idsCount; i++ {
		ids[i] = rand.Uint64()
	}

	for i := 0; i < goroutines; i++ {
		go func() {
			for {
				client.GetByIDs(testQueueID, ids)
				atomic.AddUint64(&current, 1)
			}
		}()
	}

	for {
		time.Sleep(time.Second)
		cur := atomic.LoadUint64(&current)
		fmt.Println(cur-prev, "req/s")
		prev = cur
	}
}

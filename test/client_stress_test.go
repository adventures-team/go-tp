//go:build stress
// +build stress

package tptest

import (
	"log/slog"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	advslog "github.com/adventures-team/go-adv-slog"
	"github.com/adventures-team/go-queuedproto"
)

const (
	goroutines = 10
	idsCount   = 100
)

func TestIProtoStress(t *testing.T) {
	var prev, current uint64
	ctx, client := getTestClient(t)

	advslog.Level.Set(slog.LevelDebug)

	ids := make([]queuedproto.EventID, idsCount)
	for i := uint(0); i < idsCount; i++ {
		ids[i] = queuedproto.EventID{
			ID:    rand.Uint64(),
			Shard: i % client.ShardNum(),
		}
	}

	for i := 0; i < goroutines; i++ {
		go func() {
			for {
				client.GetByIDs(ctx, testRawQueueID, ids)
				atomic.AddUint64(&current, 1)
			}
		}()
	}

	for {
		time.Sleep(time.Second)
		cur := atomic.LoadUint64(&current)
		slog.Info("throughput", "rps", cur-prev)
		prev = cur
	}
}

//go:build stress
// +build stress

package tptest

import (
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	zLog "github.com/rs/zerolog/log"

	"github.com/my-mail-ru/go-queuedproto"
)

const (
	goroutines = 10
	idsCount   = 100
)

func TestIProtoStress(t *testing.T) {
	var prev, current uint64
	ctx, client := getTestClient(t)

	zerolog.SetGlobalLevel(zerolog.DebugLevel)

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
		zLog.Info().Uint64("rps", cur-prev).Send()
		prev = cur
	}
}

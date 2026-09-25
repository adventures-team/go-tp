//go:build integration

package tptest

import (
	"context"
	"errors"
	"math/rand"
	"time"

	advslog "github.com/adventures-team/go-adv-slog"
	"github.com/adventures-team/go-queuedproto/addons"

	"github.com/adventures-team/go-tp"
)

// used by TestInitialDelay only
const testInitialDelay = 5 * time.Second

type testService struct {
	Counter *counter
}

// eventSleeper - работает долго, и не всегда успешно
//
//adv:iproto:
type eventSleeper struct {
	tp.BaseEvent
	addons.CreationTime

	DurationMin time.Duration
	DurationMax time.Duration
	Percentage  uint8
}

var _ tp.Event = &eventSleeper{}

func (eventSleeper) Version() uint8 {
	return 1
}

func (eventSleeper) QueueID() uint16 {
	return testSleeperQueueID
}

func (svc *testService) HandleSleeper(ctx context.Context, events []eventSleeper) error {
	zLog := advslog.Ctx(ctx)

	svc.Counter.Inc()
	defer svc.Counter.Dec()

	for _, ev := range events {
		dur := ev.DurationMin
		if dur == 0 && ev.DurationMax == 0 {
			ev.Done()

			// just check CreationTime and return, don't sleep
			delay := time.Since(time.Unix(int64(ev.CreationTime), 0))
			if delay < testInitialDelay {
				zLog.Error("initial delay is too short", "actual_delay", delay)
				panic("initial delay is too short")
			}

			zLog.Info("initial delay", "actual_delay", delay)
			svc.Counter.Done()

			continue
		}

		if ev.DurationMin != ev.DurationMax {
			dur += time.Duration(rand.Intn(int(ev.DurationMax - ev.DurationMin)))
		}

		zLog.Info("eventSleeper: sleeping...", "duration", dur)
		time.Sleep(dur)

		if uint8(rand.Intn(100)) < ev.Percentage {
			zLog.Info("eventSleeper: OK")
			ev.Done() // в случае паники в след. строке (специально устанавливаем низкое значение счётчика) событие всё равно должно удалиться
			svc.Counter.Done()

			if !ev.IsDone() {
				return errors.New("IsDone() returned false after calling Done()")
			}
		} else {
			zLog.Info("eventSleeper: failed")
		}
	}

	return nil
}

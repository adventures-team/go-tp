//go:build integration
// +build integration

package tptest

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/rs/zerolog"

	"gitlab.corp.mail.ru/mygo/tp.git"
)

type testService struct {
	Counter *counter
}

// eventSleeper - работает долго, и не всегда успешно
type eventSleeper struct {
	tp.BaseEvent

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
	zLog := zerolog.Ctx(ctx)

	svc.Counter.Inc()
	defer svc.Counter.Dec()

	for _, ev := range events {
		dur := ev.DurationMin
		if ev.DurationMin != ev.DurationMax {
			dur += time.Duration(rand.Intn(int(ev.DurationMax - ev.DurationMin)))
		}

		zLog.Info().Msgf("eventSleeper: sleeping %v...", dur)
		time.Sleep(dur)

		if uint8(rand.Intn(100)) < ev.Percentage {
			zLog.Info().Msg("eventSleeper: OK")
			ev.Done() // в случае паники в след. строке (специально устанавливаем низкое значение счётчика) событие всё равно должно удалиться
			svc.Counter.Done()

			if !ev.IsDone() {
				return errors.New("IsDone() returned false after calling Done()")
			}
		} else {
			zLog.Info().Msg("eventSleeper: failed")
		}
	}

	return nil
}

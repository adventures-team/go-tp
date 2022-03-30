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

// eventSleeper - работает долго, и не всегда успешно
type eventSleeper struct {
	tp.Event
	Counter *counter

	DurationMin time.Duration
	DurationMax time.Duration
	Percentage  uint8
}

var _ tp.Handler = &eventSleeper{}

func (*eventSleeper) Version() uint8 {
	return 1
}

func (ev *eventSleeper) Handle(ctx context.Context) error {
	zLog := zerolog.Ctx(ctx)

	ev.Counter.Inc()
	defer ev.Counter.Dec()

	dur := ev.DurationMin + time.Duration(rand.Intn(int(ev.DurationMax-ev.DurationMin)))

	zLog.Info().Msgf("eventSleeper: sleeping %v...", dur)
	time.Sleep(dur)

	if uint8(rand.Intn(100)) < ev.Percentage {
		zLog.Info().Msg("eventSleeper: OK")
		ev.Done() // в случае паники в след. строке (специально устанавливаем низкое значение счётчика) событие всё равно должно удалиться
		ev.Counter.Done()

		return nil
	}

	zLog.Info().Msg("eventSleeper: failed")

	return errors.New("eventSleeper has decided to fail")
}

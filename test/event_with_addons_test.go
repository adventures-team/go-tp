//go:build integration

package tptest

import (
	"context"

	"github.com/rs/zerolog"

	"github.com/adventures-team/go-queuedproto/addons"

	"gitlab.adv.team/go/tp.git"
)

type (
	// eventWithAddons - с аддонами, детерминированным ID и игнором дублей
	//adv:iproto:
	eventWithAddons struct {
		tp.BaseEvent
		addons.CreationTime
		addons.Producer

		TrackID uint32
		ExternalID   uint32
	}

	// eventWithAddonsV2 - тест на событие новой версии
	// TODO сделать потом рекурсивный перебор полей во вложенных структурах (делается при старте/наполнении кеша - пофиг, что это долго)
	// чтобы можно было встраивать структуры при добавлении полей в новой версии).
	//adv:iproto:
	eventWithAddonsV2 struct {
		tp.BaseEvent
		addons.CreationTime
		addons.Producer

		TrackID uint32
		ExternalID   uint32
		Name    string `iproto:"ber"`
	}
)

var (
	_ tp.Event                = &eventWithAddons{}
	_ tp.Builder              = &eventWithAddons{}
	_ tp.EventIDBuilder       = &eventWithAddons{}
	_ tp.IgnoreDuplicateItems = &eventWithAddons{}
)

func (eventWithAddons) Version() uint8 {
	return 1
}

func (eventWithAddons) QueueID() uint16 {
	return testAddonsQueueID
}

func (eventWithAddonsV2) Version() uint8 {
	return 2
}

func (eventWithAddonsV2) QueueID() uint16 {
	return testAddonsQueueID
}

func (ev *eventWithAddons) Build() error {
	_ = ev.CreationTime.Build()
	return ev.Producer.Build()
}

func (ev *eventWithAddons) BuildEventID() uint64 {
	return uint64(ev.ExternalID)<<32 | uint64(ev.TrackID)
}

// очевидно, этот метод тестим на событии с BuildEventID
func (*eventWithAddons) IgnoreDuplicateItems() bool {
	return true
}

func (ev *eventWithAddonsV2) Build() error {
	_ = ev.CreationTime.Build()
	return ev.Producer.Build()
}

func (ev *eventWithAddonsV2) BuildEventID() uint64 {
	return uint64(ev.ExternalID)<<32 | uint64(ev.TrackID)
}

func (*eventWithAddonsV2) IgnoreDuplicateItems() bool {
	return true
}

func (svc testService) HandleEventWithAddons(ctx context.Context, events []eventWithAddons) error {
	zerolog.Ctx(ctx).Info().Int("batch_len", len(events)).Msg("[]eventWithAddons")

	for _, ev := range events {
		zerolog.Ctx(ctx).Info().Interface("event", ev).Send()
		ev.Done()
		svc.Counter.Done()
	}

	return nil
}

// не выпиливать ) тест на перемешанные события разных версий
func (svc testService) HandleEventWithAddonsV2(ctx context.Context, events []eventWithAddonsV2) error {
	zerolog.Ctx(ctx).Info().Int("batch_len", len(events)).Msg("[]eventWithAddonsV2")

	for _, ev := range events {
		zerolog.Ctx(ctx).Info().Interface("event", ev).Send()
		ev.Done()
		svc.Counter.Done()
	}

	return nil
}

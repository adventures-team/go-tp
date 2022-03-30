//go:build integration
// +build integration

package tptest

import (
	"context"

	"github.com/rs/zerolog"

	"gitlab.corp.mail.ru/mygo/queuedproto.git/addons"
	"gitlab.corp.mail.ru/mygo/tp.git"
)

type (
	// eventWithAddons - с аддонами, детерминированным ID и игнором дублей
	eventWithAddons struct {
		tp.Event
		Counter *counter

		addons.CreationTime
		addons.Producer

		TrackID uint32
		ExternalID   uint32
	}

	// eventWithAddonsV2 - тест на событие новой версии
	// TODO сделать потом рекурсивный перебор полей во вложенных структурах (делается при старте/наполнении кеша - пофиг, что это долго)
	// чтобы можно было встраивать структуры при добавлении полей в новой версии).
	eventWithAddonsV2 struct {
		tp.Event
		Counter *counter

		addons.CreationTime
		addons.Producer

		TrackID uint32
		ExternalID   uint32
		Name    string
	}
)

var (
	_ tp.Handler              = &eventWithAddons{}
	_ tp.Builder              = &eventWithAddons{}
	_ tp.EventIDBuilder       = &eventWithAddons{}
	_ tp.IgnoreDuplicateItems = &eventWithAddons{}
)

func (*eventWithAddons) Version() uint8 {
	return 1
}

func (*eventWithAddonsV2) Version() uint8 {
	return 2
}

func (ev *eventWithAddons) Build() error {
	ev.CreationTime.Build()
	return ev.Producer.Build()
}

func (ev *eventWithAddons) BuildEventID() uint64 {
	return uint64(ev.ExternalID)<<32 | uint64(ev.TrackID)
}

// очевидно, этот метод тестим на событии с BuildEventID
func (*eventWithAddons) IgnoreDuplicateItems() bool {
	return true
}

// TODO см коммент у типа - выпилить

func (ev *eventWithAddonsV2) Build() error {
	ev.CreationTime.Build()
	return ev.Producer.Build()
}

func (ev *eventWithAddonsV2) BuildEventID() uint64 {
	return uint64(ev.ExternalID)<<32 | uint64(ev.TrackID)
}

func (*eventWithAddonsV2) IgnoreDuplicateItems() bool {
	return true
}

func (ev *eventWithAddons) Handle(ctx context.Context) error {
	zerolog.Ctx(ctx).Info().Int("batch_len", ev.BatchLen()).Interface("event", ev).Msgf("event_id=0x%016X", ev.ID())
	ev.Done()
	ev.Counter.Done()

	for nev := ev; tp.Next(&nev); {
		zerolog.Ctx(ctx).Info().Interface("event", nev).Msgf("Next: event_id=0x%016X", nev.ID())
		nev.Done()
		ev.Counter.Done()
	}

	return nil
}

// не выпиливать ) тест на перемешанные события разных версий
func (ev *eventWithAddonsV2) Handle(ctx context.Context) error {
	zerolog.Ctx(ctx).Info().Int("batch_len", ev.BatchLen()).Interface("event", ev).Msgf("V2 event_id=0x%016X", ev.ID())
	ev.Done()
	ev.Counter.Done()

	for nev := ev; tp.Next(&nev); {
		zerolog.Ctx(ctx).Info().Interface("event", nev).Msgf("V2 Next: event_id=0x%016X", nev.ID())
		nev.Done()
		ev.Counter.Done()
	}

	return nil
}

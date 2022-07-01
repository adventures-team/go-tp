//go:build !integration
// +build !integration

package tp

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	zLog "github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"

	"gitlab.corp.mail.ru/mygo/queuedproto.git"
)

type (
	testEvent struct {
		Event
	}

	testAnotherEvent struct {
		Event
	}

	testUnhandledEvent struct {
		NoHandler
	}
)

var (
	_ Handler = &testEvent{}
	_ Handler = &testAnotherEvent{}
	_ Handler = &testUnhandledEvent{}
)

func (testEvent) Version() uint8 {
	return 1
}

func (testAnotherEvent) Version() uint8 {
	return 1
}

func (testUnhandledEvent) Version() uint8 {
	return 1
}

func (*testEvent) Handle(context.Context) error {
	return nil
}

func (*testAnotherEvent) Handle(context.Context) error {
	return nil
}

func getTestEvents(t *testing.T) *testEvent {
	var next Handler

	evl := &EventList{
		queueID: 12345,
		queue:   &Queue{},
	}

	setLogLevel(&zLog.Logger, zerolog.Disabled)

	for i := 0; i < 10; i++ {
		ev := &testEvent{}
		ev.EventID = queuedproto.EventID{ID: 1, Shard: 1}
		ev.next = next
		ev.list = evl
		next = ev
	}

	return next.(*testEvent)
}

func TestNext(t *testing.T) {
	ev := getTestEvents(t)
	ctx := context.Background()

	for Next(&ctx, &ev) { // просто тестируем на отсутствие паник
	}

	noHandler := &testUnhandledEvent{}
	assert.Equal(t, false, Next(&ctx, &noHandler))
}

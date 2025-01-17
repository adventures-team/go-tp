//go:build !integration
// +build !integration

package tp_test

import (
	"context"
	"testing"

	"gitlab.corp.my.games/adventures/go/easygo.git/iproto"
	"gitlab.corp.my.games/adventures/go/queuedproto.git/addons"
	tp "gitlab.corp.my.games/adventures/go/tp.git"
)

type (
	// TestAPI - пример типа с объектами для доступа к базам
	TestAPI struct {
		tp.DisableMarshaler
		DB    interface{}
		MyBox interface{}
	}

	// testEvent - пример события с объектом API, которому отключён маршалер, но это не должно мешать отключению
	// маршалинга всего события
	testEvent struct {
		tp.BaseEvent
		TestAPI
		UserID uint32
	}

	// testUnhandledEvent - пример события только для публикации со встроенными аддонами, маршалеры которых
	// не должны быть доступны для объекта события
	testUnhandledEvent struct {
		tp.BaseEvent
		addons.Retry
		UserID uint32
	}

	// testUnhandledEvent2 - то же самое, но у CreationTime по-другому вкорячены маршалеры (без встраивания)
	// не должны быть доступны для объекта события
	testUnhandledEvent2 struct {
		tp.BaseEvent
		addons.CreationTime
		UserID uint32
	}
)

var (
	_ tp.Event = &testEvent{}
	_ tp.Event = &testUnhandledEvent{}
	_ tp.Event = &testUnhandledEvent2{}
)

func (testEvent) QueueID() uint16 {
	return 1
}

func (testEvent) Version() uint8 {
	return 1
}

func (testUnhandledEvent) QueueID() uint16 {
	return 1
}

func (testUnhandledEvent) Version() uint8 {
	return 1
}

func (testUnhandledEvent2) QueueID() uint16 {
	return 1
}

func (testUnhandledEvent2) Version() uint8 {
	return 1
}

func (*testEvent) Handle(context.Context) error {
	return nil
}

func checkMarshalers(t *testing.T, val interface{}) {
	_, ok := val.(iproto.IprotoPacker)
	if ok {
		t.Errorf("%T must not implement iproto.IprotoPacker", val)
	}

	_, ok = val.(iproto.IprotoUnpacker)
	if ok {
		t.Errorf("%T must not implement iproto.IprotoUnpacker", val)
	}

	_, ok = val.(addons.PerlAddon)
	if ok {
		t.Errorf("%T must not implement addons.PerlAddon", val)
	}
}

func TestDeleteMarshalers(t *testing.T) {
	checkMarshalers(t, &testEvent{})
	checkMarshalers(t, &testUnhandledEvent{})
	checkMarshalers(t, &testUnhandledEvent2{})
}

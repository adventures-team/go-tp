//go:build !integration
// +build !integration

package tp_test

import (
	"context"
	"testing"

	"github.com/funny-falcon/go-iproto/marshal"

	"gitlab.corp.mail.ru/mygo/queuedproto.git/addons"
	"gitlab.corp.mail.ru/mygo/tp.git"
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
		tp.Event
		TestAPI
		UserID uint32
	}

	// testUnhandledEvent - пример события только для публикации со встроенными аддонами, маршалеры которых
	// не должны быть доступны для объекта события
	testUnhandledEvent struct {
		tp.NoHandler
		addons.Retry
		UserID uint32
	}

	// testUnhandledEvent2 - то же самое, но у CreationTime по-другому вкорячены маршалеры (без встраивания)
	// не должны быть доступны для объекта события
	testUnhandledEvent2 struct {
		tp.NoHandler
		addons.CreationTime
		UserID uint32
	}
)

var (
	_ tp.Handler = &testEvent{}
	_ tp.Handler = &testUnhandledEvent{}
	_ tp.Handler = &testUnhandledEvent2{}
)

func (testEvent) Version() uint8 {
	return 1
}

func (testUnhandledEvent) Version() uint8 {
	return 1
}

func (testUnhandledEvent2) Version() uint8 {
	return 1
}

func (*testEvent) Handle(context.Context) error {
	return nil
}

func checkMarshalers(t *testing.T, val interface{}) {
	_, ok := val.(marshal.IShortReader)
	if ok {
		t.Errorf("%T must not implement marshal.IShortReader", val)
	}

	_, ok = val.(marshal.IShortWriter)
	if ok {
		t.Errorf("%T must not implement marshal.IShortWriter", val)
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

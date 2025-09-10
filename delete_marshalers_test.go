//go:build !integration

package tp_test

import (
	"testing"

	"github.com/my-mail-ru/go-queuedproto/addons"

	tp "gitlab.cebc.tools/go/tp.git"
)

type (
	// TestAPI - пример типа с объектами для доступа к базам
	TestAPI struct {
		tp.DisableMarshaler
		DB    any
		MyBox any
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

func checkMarshalers(t *testing.T, val any) {
	if _, ok := val.(addons.PerlAddon); ok {
		t.Errorf("%T must not implement addons.PerlAddon", val)
	}
}

func TestDeleteMarshalers(t *testing.T) {
	checkMarshalers(t, &testEvent{})
	checkMarshalers(t, &testUnhandledEvent{})
	checkMarshalers(t, &testUnhandledEvent2{})
}

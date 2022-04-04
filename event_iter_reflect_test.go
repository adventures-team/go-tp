//go:build !go1.18 && !integration
// +build !go1.18,!integration

// Для версии на дженериках ErrPointerRequired невозможна, а остальные паники менее вероятны,
// поэтому прогоняем тест лишь для рефлективной версии.

package tp

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TODO послать автору assert?
func panicsWithErrorIs(t *testing.T, err error, f func(), name string) {
	defer func() {
		reason := recover()
		if reason == nil {
			t.Fatalf("%s: should panic with %v", name, err)
		}

		reasonErr, ok := reason.(error)
		if !ok || !errors.Is(reasonErr, err) {
			t.Fatalf("%s: got %v, expected %v", name, reason, err)
		}
	}()

	f()
}

func TestNextPanic(t *testing.T) {
	ev := getTestEvents(t)
	ctx := context.Background()

	assert.PanicsWithValue(t, ErrPointerRequired, func() {
		// Нужен указатель на указатель, а не просто указатель на событие.
		// Думаю, это будет ошибка номер 1 в обработчиках :)
		Next(&ctx, ev)
	})

	assert.PanicsWithValue(t, ErrPointerRequired, func() {
		// Передать событие по значению, при вызове метода Handle по указателю - более тонкое извращение
		Next(&ctx, *ev)
	})

	panicsWithErrorIs(t, ErrEventTypesDiffer, func() {
		ev.next = &testAnotherEvent{}
		Next(&ctx, &ev)
	}, "different event types in batch")

	assert.PanicsWithValue(t, ErrMisconstructedEvent, func() {
		ev.next = (*testEvent)(nil)
		Next(&ctx, &ev)
	})
}

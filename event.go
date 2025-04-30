package tp

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/my-mail-ru/go-queuedproto"
)

const (
	evIsDeleted      uint32 = 1 << iota // события с этим флагом удаляются балком методом EventList.commit
	evHasCustomDefer                    // проставляется для событий, которым вручную был задан кастомный дефер. их не обрабатывает EventList.commit
)

type (
	// BaseEvent - базовый тип события, должен встраиваться в пользовательские типы событий.
	BaseEvent struct {
		eventInternal `iproto:"-" json:"-"`
	}

	eventInternal struct {
		queuedproto.EventID            // состоит из ID события (ID), и номера шарда (Shard). поддерживает интерфейс fmt.Stringer
		list                *EventList // список всех событий, полученных одной пачкой (т.е. разные версии для той же очереди)
		flags               *atomic.Uint32
	}
)

// NewBaseEvent - для использования в тестах. Возвращает проинициализированный BaseEvent, в котором будут работать флаги (в частности, метод Done).
// Для отправки инициализировать событие не нужно, функция предусмотрена только для тестов, которым нужно проверить установку Done.
func NewBaseEvent() BaseEvent {
	return BaseEvent{
		eventInternal: eventInternal{
			flags: new(atomic.Uint32),
		},
	}
}

type baseEventGetter interface {
	getBaseEvent() *BaseEvent
}

func (bev *BaseEvent) getBaseEvent() *BaseEvent {
	return bev
}

// GetEventID - возвращает ID события.
// Код, работающий с конкретным событием (не через интерфейс) может обращаться к полу EventID напрямую.
func (bev BaseEvent) GetEventID() queuedproto.EventID { //nolint:unused
	return bev.EventID
}

// fromQueueID - ID очереди, из которой прилетело событие
// метод нужный, не выпиливать, просто golangci-lint не умеет в приватные интерфейсы.
func (bev BaseEvent) fromQueueID() QueueIDType { //nolint:unused
	if bev.list == nil {
		return 0
	}

	return bev.list.queueID
}

// Done помечает событие обработанным.
//
// События удаляются не сразу, а по завершении цикла обработки всех событий, полученных за один запрос из queued.
// В случае возникновения паник (в т.ч. необработанных), удаление все равно должно произойти до выхода из tp.Queue.Work
func (bev BaseEvent) Done() {
	if bev.flags != nil {
		oldFlags := bev.flags.Load()
		newFlags := oldFlags | evIsDeleted

		if !bev.flags.CompareAndSwap(oldFlags, newFlags) {
			bev.Done()
		}
	}
}

// IsDone - вызывался ли метод Done для события.
func (bev BaseEvent) IsDone() bool {
	if bev.flags == nil {
		return false
	}

	return bev.flags.Load()&evIsDeleted != 0
}

// HasCustomDefer - установлено ли явно время повторной активации вызовом методов [BaseEvent.Defer] или [BaseEvent.Replace]
func (bev BaseEvent) HasCustomDefer() bool {
	if bev.flags == nil {
		return false
	}

	return bev.flags.Load()&evHasCustomDefer != 0
}

func (bev BaseEvent) setHasCustomDefer() {
	oldFlags := bev.flags.Load()
	newFlags := oldFlags&^evIsDeleted | evHasCustomDefer

	if !bev.flags.CompareAndSwap(oldFlags, newFlags) {
		bev.setHasCustomDefer()
	}
}

// Defer устанавливает задержку события.
//
// Время задержки устанавливается сразу же. По завершении цикла обработки событиям, для которых не был вызван метод Done
// либо Defer, проставляется задержка по умолчанию.
//
// Если событие было помечено как выполненное при помощи Done, а потом был вызван Defer, пометка на удаление снимается.
func (bev BaseEvent) Defer(ctx context.Context, delay time.Duration) error {
	if err := bev.list.queue.client.Defer(ctx, bev.list.queueID, delay, []queuedproto.EventID{bev.EventID}); err != nil {
		return err
	}

	if bev.flags != nil {
		bev.setHasCustomDefer()
	}

	return nil
}

func (bev BaseEvent) Queue() *Queue {
	return bev.list.queue
}

// Publish - враппер к Queue.Publish (см. его доку для описания параметра и возвращаемого значения). Отправляет новое событие в очередь, указанную при регистрации для этого типа методом Queue.Register
func (bev BaseEvent) Publish(ctx context.Context, data Event) (eventID queuedproto.EventID, err error) {
	return bev.list.queue.Publish(ctx, data)
}

// MarshalIProto - не поддерживается. Тип BaseEvent не предназначен для передачи по протоколу
func (BaseEvent) MarshalIProto(incompatible) {}

// UnmarshalIProto - не поддерживается. Тип BaseEvent не предназначен для передачи по протоколу
func (BaseEvent) UnmarshalIProto(incompatible) {}

// MarshalAddon - не поддерживается. Тип BaseEvent не предназначен для передачи по протоколу
func (BaseEvent) MarshalAddon(incompatible) {}

// UnmarshalAddon - не поддерживается. Тип BaseEvent не предназначен для передачи по протоколу
func (BaseEvent) UnmarshalAddon(incompatible) {}

// AddonID - не поддерживается. Тип BaseEvent не предназначен для передачи по протоколу
func (BaseEvent) AddonID(incompatible) {}

func (BaseEvent) MarshalerDisabled(incompatible) {} //nolint:unused

// EventList - список событий, прилетающих из queued для одной очереди.
// TODO подумать про перенос в internal, чтобы не путать разработчиков. Сущность внутренняя.
type EventList struct {
	queueID         QueueIDType
	baseEvents      []*BaseEvent        // указатели на кишки внутри всех событий, полученных для этой очереди за раз от queued
	eventsByVersion [MaxVersion + 1]any // слайсы конкретных типов юзерского события, сгруппированные по версиям
	queue           *Queue
}

// commit удаляет события, для которых был вызван метод Done, а также проставляет дефолтный дефер (defer_time из конфига)
// всем событиям из списка, которые не были удалены, и для которых не был вызван метод Defer с кастомным временем.
func (evl *EventList) commit(ctx context.Context, stat *Stat) error {
	deleteEventIDs := make([]queuedproto.EventID, 0, len(evl.baseEvents))
	deferEventIDs := make([]queuedproto.EventID, 0, len(evl.baseEvents))
	deferred := uint32(0)

	for _, ev := range evl.baseEvents {
		if ev.flags == nil {
			return fmt.Errorf("event %v was not received from the queue", ev.EventID)
		}

		flags := ev.flags.Load()
		if flags&evIsDeleted != 0 {
			deleteEventIDs = append(deleteEventIDs, ev.EventID)
		} else {
			if flags&evHasCustomDefer == 0 {
				deferEventIDs = append(deferEventIDs, ev.EventID)
			}

			deferred++
		}
	}

	stat.processed.Add(uint32(len(evl.baseEvents)))
	stat.errors.Add(deferred) // TODO в новой стате это должно идти как deferred

	if err := evl.queue.client.Delete(ctx, evl.queueID, deleteEventIDs); err != nil {
		return err
	}

	return evl.queue.client.Defer(ctx, evl.queueID, 0, deferEventIDs)
}

package tp

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"gitlab.corp.mail.ru/mygo/queuedproto.git"
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

// baseEventGetter - метод getBaseEvent не включён в интерфейс Event, т.к. не всегда
// указатель на тип присутствует в явном виде, несмотря на то, что возможность вызова
// по указателю есть (пример - []UserEvent).
type baseEventGetter interface {
	getBaseEvent() *BaseEvent
}

// getBaseEvent - доступ к кишкам события из юзерской структуры
func (ev *BaseEvent) getBaseEvent() *BaseEvent {
	return ev
}

// getEventID - BaseEvent принимается по значению по причине, описанной для [baseEventGetter].
// метод нужный, не выпиливать, просто golangci-lint не умеет в приватные интерфейсы.
func (ev BaseEvent) getEventID() queuedproto.EventID { //nolint:unused
	return ev.EventID
}

// FromQueueID - ID очереди, из которой прилетело событие
func (ev *BaseEvent) FromQueueID() QueueIDType {
	return ev.list.queueID
}

// Done помечает событие обработанным.
//
// События удаляются не сразу, а по завершении цикла обработки всех событий, полученных за один запрос из queued.
// В случае возникновения паник (в т.ч. необработанных), удаление все равно должно произойти до выхода из tp.Queue.Work
func (ev *BaseEvent) Done() {
	if ev.flags != nil {
		oldFlags := ev.flags.Load()
		newFlags := oldFlags | evIsDeleted

		if !ev.flags.CompareAndSwap(oldFlags, newFlags) {
			ev.Done()
		}
	}
}

// IsDone - вызывался ли метод Done для события.
func (ev *BaseEvent) IsDone() bool {
	if ev.flags == nil {
		return false
	}

	return ev.flags.Load()&evIsDeleted != 0
}

// Defer устанавливает задержку события.
//
// Время задержки устанавливается сразу же. По завершении цикла обработки событиям, для которых не был вызван метод Done
// либо Defer, проставляется задержка по умолчанию.
//
// Если событие было помечено как выполненное при помощи Done, а потом был вызван Defer, пометка на удаление снимается.
func (ev *BaseEvent) Defer(ctx context.Context, delay time.Duration) error {
	if err := ev.list.queue.client.Defer(ctx, ev.list.queueID, delay, []queuedproto.EventID{ev.EventID}); err != nil {
		return err
	}

	if ev.flags != nil {
		ev.setHasCustomDefer()
	}

	return nil
}

func (ev *BaseEvent) setHasCustomDefer() {
	oldFlags := ev.flags.Load()
	newFlags := oldFlags&^evIsDeleted | evHasCustomDefer

	if !ev.flags.CompareAndSwap(oldFlags, newFlags) {
		ev.setHasCustomDefer()
	}
}

// Publish - враппер к Queue.Publish (см. его доку для описания параметра и возвращаемого значения). Отправляет новое событие в очередь, указанную при регистрации для этого типа методом Queue.Register
func (ev *BaseEvent) Publish(ctx context.Context, data Event) (eventID queuedproto.EventID, err error) {
	return ev.list.queue.Publish(ctx, data)
}

// IprotoPack - не поддерживается. Тип BaseEvent не предназначен для передачи по протоколу
func (BaseEvent) IprotoPack(incompatible) {}

// IprotoUnpack - не поддерживается. Тип BaseEvent не предназначен для передачи по протоколу
func (BaseEvent) IprotoUnpack(incompatible) {}

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

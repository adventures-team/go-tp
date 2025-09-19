package tp

import (
	"context"
	"fmt"
	"hash/crc32"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/my-mail-ru/go-iproto"
	"github.com/my-mail-ru/go-queuedproto"
)

const (
	evIsDeleted      uint32 = 1 << iota // события с этим флагом удаляются балком методом eventList.commit
	evHasCustomDefer                    // проставляется для событий, которым вручную был задан кастомный дефер. их не обрабатывает eventList.commit
)

type (
	// Event - интерфейс обработчика очереди. Структура обработчика должна встраивать тип BaseEvent.
	//
	// Нужно определить два метода - Version и QueueID, остальные унаследуются от встроенного BaseEvent.
	Event interface {
		// Marshaler используется для кодирования события при отправке. Должен быть
		// реализован/сгенерён для типа события.
		// Если не реализовать/сгенерить Unmarshaler, обрабатываться такие события
		// не будут (их не получится зарегистрировать вызовом [Register]),
		// доступна будет только отправка (Publish)
		iproto.Marshaler

		// Version нужно определить для типа события.
		Version() VersionType

		// QueueID нужно определить для типа события.
		QueueID() QueueIDType

		// Shard принимает количество шардов, и должен возвращать номер
		// шарда queued, в который будет отправлено событие. Обычно
		// для этого используется ID события.
		//
		// В BaseEvent определён метод Shard, совместимый с перловой реализацией.
		// Тип события может переопределить метод шардирования.
		Shard(shardNum uint) uint

		// GetEventID возвращает ID события.
		//
		// Определён в BaseEvent.
		GetEventID() queuedproto.EventID

		// Queue возвращает интерфейс [Queue] для работы с API очередей.
		//
		// Определён в BaseEvent.
		Queue() Queue

		// Defer откладывает следующую активацию события на указзанную задержку.
		// Выполняется сразу, а не в конце обработки пачки событий (как это
		// происходит при вызове Done, или при дефолтном дефере).
		//
		// Определён в BaseEvent.
		Defer(context.Context, time.Duration) error

		// HasCustomDefer возвращает признак, был ли вызван метод [Defer] явно.
		//
		// Определён в BaseEvent.
		HasCustomDefer() bool

		// Done помечает событие успешно обработанным. Такие события удаляются из очереди
		// в конце цикла обработки пачки событий, полученных из очереди (под пачкой имеются
		// ввиду все события из одной очереди, включая все их версии - т.е. потенциально
		// больше, чем размер слайса, переданного обработчику). Удаление выполняется
		// одним запросом.
		//
		// Определён в BaseEvent.
		Done()

		// IsDone возвращает признак успешного выполнения события.
		//
		// Определён в BaseEvent.
		IsDone() bool
	}

	// BaseEvent - базовый тип события, должен встраиваться в пользовательские типы событий.
	BaseEvent struct {
		eventInternal `iproto:"-" json:"-"`
	}

	eventInternal struct {
		queuedproto.EventID            // состоит из ID события (ID), и номера шарда (Shard). поддерживает интерфейс fmt.Stringer
		list                *eventList // список всех событий, полученных одной пачкой (т.е. разные версии для той же очереди)
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

// HasCustomDefer - установлено ли явно время повторной активации вызовом методов [BaseEvent.Defer] или [Queue.Replace]
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
	if err := bev.list.queue.Client().Defer(ctx, bev.list.queueID, delay, []queuedproto.EventID{bev.EventID}); err != nil {
		return err
	}

	if bev.flags != nil {
		bev.setHasCustomDefer()
	}

	return nil
}

// Queue возвращает интерфейс API очередей.
func (bev BaseEvent) Queue() Queue {
	return bev.list.queue
}

// Shard распределяет события по шардам в зависимости от хеша от ID события.
// Совместимо с перлом (алгоритм хеширования - crc от строкового десятичного
// представления eventID, распределение по шардам - остаток от деления).
func (bev BaseEvent) Shard(shardNum uint) uint {
	hash := uint64(crc32.ChecksumIEEE([]byte(strconv.FormatUint(bev.EventID.ID, 10))))
	return uint(hash % uint64(shardNum))
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

// eventList - список событий, прилетающих из queued для одной очереди.
// TODO подумать про перенос в internal, чтобы не путать разработчиков. Сущность внутренняя.
type eventList struct {
	queueID         QueueIDType
	baseEvents      []*BaseEvent        // указатели на кишки внутри всех событий, полученных для этой очереди за раз от queued
	eventsByVersion [MaxVersion + 1]any // слайсы конкретных типов юзерского события, сгруппированные по версиям
	queue           Queue
}

// commit удаляет события, для которых был вызван метод Done, а также проставляет дефолтный дефер (defer_time из конфига)
// всем событиям из списка, которые не были удалены, и для которых не был вызван метод Defer с кастомным временем.
func (evl *eventList) commit(ctx context.Context, stat *Stat) error {
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

	if err := evl.queue.Client().Delete(ctx, evl.queueID, deleteEventIDs); err != nil {
		return err
	}

	return evl.queue.Client().Defer(ctx, evl.queueID, 0, deferEventIDs)
}

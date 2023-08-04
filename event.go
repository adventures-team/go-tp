package tp

import (
	"context"
	"hash/crc32"
	"strconv"
	"time"

	"gitlab.corp.mail.ru/mygo/queuedproto.git"
)

const (
	evIsDeleted      uint8 = 1 << iota // события с этим флагом удаляются балком методом EventList.commit
	evHasCustomDefer                   // проставляется для событий, которым вручную был задан кастомный дефер. их не обрабатывает EventList.commit
	evIsProcessed                      // проставляется функцией tp.Next для возвращаемых ей событий. для пропуска уже обработанных событий в основном цикле. НЕ означает, что событие обработано успешно
)

type (
	// Event - высокоуровневое представление события. Предоставлены также врапперы к нужным частям API очередей.
	Event struct {
		eventInternal `iproto:"-" json:"-"`
	}

	// NoHandler - если для очереди (пока) не предусмотрена обработка (например, обработчик реализован на перле),
	// надо встроить в структуру события этот тип
	NoHandler struct {
		Event
	}

	noHandler interface {
		noHandler()
	}

	// next это слишком C-style, особенно вместе с костыльным логгированием. Кажется, более красиво это реализуется через канал в Queue.Work.
	eventInternal struct {
		queuedproto.EventID            // состоит из ID события (ID), и номера шарда (Shard). поддерживает интерфейс fmt.Stringer
		list                *EventList // список всех событий, полученных одной пачкой (т.е. разные версии для той же очереди)
		next                Handler    // следующее событие того же типа (т.е. той же версии для той же очереди)
		flags               uint8      // см. константы ev* выше
	}

	// EventList - список событий, прилетающих из queued для одной очереди. В списке могут быть события разных версий.
	EventList struct {
		queueID QueueIDType
		events  []Handler
		queue   *Queue
	}
)

var (
	_ Hasher  = &Event{}
	_ Sharder = &Event{}
)

// GetEvent - доступ к кишкам события из юзерской структуры
func (ev *Event) GetEvent() *Event {
	return ev
}

// QueueID - ID очереди, из которой прилетело событие
func (ev *Event) QueueID() QueueIDType {
	return ev.list.queueID
}

// Hash - возвращает хэш от ID события, совместимый с перловыми очередями (crc32 от строкового десятичного представления ID)
func (ev *Event) Hash() uint64 {
	return uint64(crc32.ChecksumIEEE([]byte(strconv.FormatUint(ev.EventID.ID, 10))))
}

// Shard - распределяет событие по шардам в зависимости от хеша. Совместимая с перлом версия (остаток от деления).
func (ev *Event) Shard(hash uint64) uint {
	return uint(hash % uint64(ev.list.queue.client.ShardNum()))
}

// Done помечает событие обработанным.
//
// События удаляются не сразу, а по завершении цикла обработки всех событий, полученных за один запрос из queued.
// В случае возникновения паник (в т.ч. необработанных), удаление все равно должно произойти до выхода из tp.Queue.Work
func (ev *Event) Done() {
	ev.flags |= evIsDeleted
}

// Defer устанавливает задержку события.
//
// Время задержки устанавливается сразу же. По завершении цикла обработки событиям, для которых не был вызван метод Done
// либо Defer, проставляется задержка по умолчанию.
//
// Если событие было помечено как выполненное при помощи Done, а потом был вызван Defer, пометка на удаление снимается.
func (ev *Event) Defer(ctx context.Context, delay time.Duration) error {
	if err := ev.list.queue.client.Defer(ctx, ev.list.queueID, delay, []queuedproto.EventID{ev.EventID}); err != nil {
		return err
	}

	ev.flags = ev.flags&^evIsDeleted | evHasCustomDefer

	return nil
}

// Publish - враппер к Queue.Publish (см. его доку для описания параметра и возвращаемого значения). Отправляет новое событие в очередь, указанную при регистрации для этого типа методом Queue.Register
func (ev *Event) Publish(ctx context.Context, data Handler) (eventID queuedproto.EventID, err error) {
	return ev.list.queue.Publish(ctx, data)
}

// BatchLen возвращает количество событий одинакового типа (т.е. кол-во раз, когда tp.Next(&ev) вернёт true + 1).
// Если событие в пачке единственное - возвращается 1.
//
//	ids := make([]uint32, 0, myev.BatchLen())
//	for {
//		ids = append(ids, myev.UserID)
//
//		if !tp.Next(&myev) {
//			break
//		}
//	}
func (ev *Event) BatchLen() int {
	l := 1

	for {
		if ev.next == nil {
			return l
		}

		l++

		ev = ev.next.GetEvent() // TODO когда-нибудь порефакторить Event/EventList, но точно после запуска в прод
	}
}

// commit удаляет события, для которых был вызван метод Done, а также проставляет дефолтный дефер (defer_time из конфига)
// всем событиям из списка, которые не были удалены, и для которых не был вызван метод Defer с кастомным временем.
func (evl *EventList) commit(ctx context.Context) error {
	deleteEventIDs := make([]queuedproto.EventID, 0, len(evl.events))
	deferEventIDs := make([]queuedproto.EventID, 0, len(evl.events))

	for _, handler := range evl.events {
		if ev := handler.GetEvent(); ev.flags&evIsDeleted != 0 {
			deleteEventIDs = append(deleteEventIDs, ev.EventID)
		} else if ev.flags&evHasCustomDefer == 0 {
			deferEventIDs = append(deferEventIDs, ev.EventID)
		}
	}

	if err := evl.queue.client.Delete(ctx, evl.queueID, deleteEventIDs); err != nil {
		return err
	}

	return evl.queue.client.Defer(ctx, evl.queueID, 0, deferEventIDs)
}

// IprotoPack - см. описание NoHandler.IprotoPack
func (Event) IprotoPack(incompatible) {}

// IprotoUnpack - см. описание NoHandler.IprotoPack
func (Event) IprotoUnpack(incompatible) {}

// MarshalAddon - см. описание NoHandler.IprotoPack
func (Event) MarshalAddon(incompatible) {}

// UnmarshalAddon - см. описание NoHandler.IprotoPack
func (Event) UnmarshalAddon(incompatible) {}

// AddonID - см. описание NoHandler.IprotoPack
func (Event) AddonID(incompatible) {}

func (Event) marshalerDisabled(incompatible) {} //nolint:unused
func (Event) enforceDictatorship()           {} //nolint:unused

// Handle - не делает ничего
func (NoHandler) Handle(context.Context) error {
	return nil
}

func (NoHandler) noHandler() {}

// IprotoPack - ломаем совместимость NoHandler с маршалерами, чтобы маршалер из вложенной структуры (например,
// DisableMarshaler из структуры объектов API, вложенный в событие), не повлиял на маршалинг события.
//
// Почему бы не объявить приватный тип с запрещёнными маршалерами, и не встроить его в типы NoHandler и Event?
// Зачем мозолить глаза разработчику, читающему доку?
//
// По правилам проброса встроенных полей в го: x.f denotes the field or method at the shallowest depth
// in T where there is such an f. If there is not exactly one f with shallowest depth, the selector
// expression is illegal.
//
// Т.е. если мы объявим "запрещающие" методы глубже (структура, вложенная в структуру Event/NoHandler) не перекроют
// методы, объявленные непосредственно у вложенной структуры 1го уровня.
//
// Если же всё-таки зачем-то потребуется объявить маршалеры для типа события - это можно сделать, т.к. они перекроют
// "наследуемые" методы (и с правильной, и с неправильной сигнатурой).
func (NoHandler) IprotoPack(incompatible) {}

// IprotoUnpack - см. описание NoHandler.IprotoPack
func (NoHandler) IprotoUnpack(incompatible) {}

// MarshalAddon - см. описание NoHandler.IprotoPack
func (NoHandler) MarshalAddon(incompatible) {}

// UnmarshalAddon - см. описание NoHandler.IprotoPack
func (NoHandler) UnmarshalAddon(incompatible) {}

// AddonID - см. описание NoHandler.IprotoPack
func (NoHandler) AddonID(incompatible) {}

func (NoHandler) marshalerDisabled(incompatible) {} //nolint:unused

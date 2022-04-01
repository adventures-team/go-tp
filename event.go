package tp

import "time"

const (
	evIsDeleted      uint8 = 1 << iota // события с этим флагом удаляются балком методом eventList.commit
	evHasCustomDefer                   // проставляется для событий, которым вручную был задан кастомный дефер. их не обрабатывает eventList.commit
	evIsProcessed                      // проставляется функцией tp.Next для возвращаемых ей событий. для пропуска уже обработанных событий в основном цикле. НЕ означает, что событие обработано успешно
)

type (
	// Event - высокоуровневое представление события. Предоставлены также врапперы к нужным частям API очередей.
	Event struct {
		eventInternal `iproto:"-" json:"-"`
	}

	// next это слишком C-style, особенно вместе с костыльным логгированием. Кажется, более красиво это реализуется через канал в Queue.Work.
	// TODO убрать nolint, когда пофиксят https://github.com/golangci/golangci-lint/issues/826
	//nolint:structcheck
	eventInternal struct {
		id    uint64     // сделано приватным (с аксессором), чтобы не вводить юзера в заблуждение (что можно как-то хитро проставить это поле, и это на что-то повлияет при отправке)
		list  *eventList // список всех событий, полученных одной пачкой (т.е. разные версии для той же очереди)
		next  Handler    // следующее событие того же типа (т.е. той же версии для той же очереди)
		flags uint8      // см. константы ev* выше
	}

	// eventList - список событий, прилетающих из queued для одной очереди. В списке могут быть события разных версий.
	eventList struct {
		queueID QueueIDType
		events  []Handler
		queue   *Queue
	}
)

// GetEvent - доступ к кишкам события из юзерской структуры
func (ev *Event) GetEvent() *Event {
	return ev
}

// ID события
func (ev *Event) ID() uint64 {
	return ev.id
}

// QueueID - ID очереди, из которой прилетело событие
func (ev *Event) QueueID() QueueIDType {
	return ev.list.queueID
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
func (ev *Event) Defer(delay time.Duration) error {
	if err := ev.list.queue.client.Defer(ev.list.queueID, delay, []uint64{ev.id}); err != nil {
		return err
	}

	ev.flags = ev.flags&^evIsDeleted | evHasCustomDefer // TODO убрать, либо документировать снятие флага удаления

	return nil
}

// Publish - враппер к Queue.Publish (см. его доку для описания параметра и возвращаемого значения). Отправляет новое событие в очередь, указанную при регистрации для этого типа методом Queue.Register
func (ev *Event) Publish(data Handler) (eventID uint64, err error) {
	return ev.list.queue.Publish(data)
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

		ev = ev.next.GetEvent() // TODO когда-нибудь порефакторить Event/eventList, но точно после запуска в прод
	}
}

// commit удаляет события, для которых был вызван метод Done, а также проставляет дефолтный дефер (defer_time из конфига)
// всем событиям из списка, которые не были удалены, и для которых не был вызван метод Defer с кастомным временем.
func (evl *eventList) commit() error {
	deleteEventIDs := make([]uint64, 0, len(evl.events))
	deferEventIDs := make([]uint64, 0, len(evl.events))

	for _, handler := range evl.events {
		if ev := handler.GetEvent(); ev.flags&evIsDeleted != 0 {
			deleteEventIDs = append(deleteEventIDs, ev.id)
		} else if ev.flags&evHasCustomDefer == 0 {
			deferEventIDs = append(deferEventIDs, ev.id)
		}
	}

	if err := evl.queue.client.Delete(evl.queueID, deleteEventIDs); err != nil {
		return err
	}

	return evl.queue.client.Defer(evl.queueID, 0, deferEventIDs)
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
func (Event) enforceDictatorship()           {}

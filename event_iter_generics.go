//go:build go1.18 && !gomarkdoc
// +build go1.18,!gomarkdoc

package tp

// Next - итерация по событиям одной версии, полученной в одной пачке.
//
// Для высоконагруженных очередей может быть неплохой выигрыш от мульти-обработки событий: мы можем запросить
// объекты по ID из коробки/базы одним запросом, более того, если набор ID небольшой (например, все юзеры смотрят
// одну какую-то очень популярную страницу), экономия трафика/нагрузки на БД будет существенна.
//
//	func (ev *MyEvent) Handler(....) ... {
//		...
//		ids := []uint32{ev.UserID}
//		for nev := ev; tp.Next(&ev); {
//			ids = append(ids, nev.UserID)
//		}
//
//		... SelectMultiByUserID(ids)
//		// заполняем map по UserID
//
//		for {
//			Process(byUserID[ev.UserID])
//			ev.Done() // вызывать Done при использовании Next обязательно
//			if !tp.Next(&ev) {
//				break
//			}
//		}
//
//		return nil
//	}
//
// Вызывать метод Done для событий, полученных при помощи Next, обязательно. Чтобы обеспечить более однообразный подход
// и исключить путаницу, возможно, выпилю неявный Done при возврате без ошибки из Handler (для первого события из пачки).
//
// Передавать надо указатель на receiver метода Handler.
//
// Версия функции для go1.18+ не кидает паники - ErrPointerRequired невозможна (возникнет ошибка при компиляции),
// ErrEventTypesDiffer и ErrMisconstructedEvent менее вероятны, чем в рефлективной версии, но всё-таки возможны
// при внутренних ошибках в tp. Если эти две паники не возникнут ни разу за время эксплуатации рефлективной версии -
// можно их тут не обрабатывать.
func Next[T Handler](evPtr *T) bool {
	next := (*evPtr).GetEvent().next

	if next == nil {
		return false
	}

	*evPtr = next.(T)
	nextEv := (*evPtr).GetEvent()
	nextEv.flags |= evIsProcessed

	nextEv.list.queue.logger.Debug().Uint16("queue_id", nextEv.QueueID()).Uint64("event_id", nextEv.id).Interface("event", *evPtr).Msg("new event received (batch mode)")

	return true
}

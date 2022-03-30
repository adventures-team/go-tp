//go:build !go1.18 || gomarkdoc
// +build !go1.18 gomarkdoc

package tp

import (
	"fmt"
	"reflect"
)

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
// При передаче неправильного значения (указатель на объект события, вместо указателя на указатель, и т.п.) произойдёт
// паника. При передаче правильного значения функция всегда отработает успешно, поэтому паники здесь не нарушают
// идиоматики языка, т.к. используются при ошибках проектирования, а не для обработки runtime-ошибок (эмуляции исключений).
//
// Все приводящие к паникам ситуации обрабатываются, и выдаётся понятное описание паники (а не общие описания
// от пакета reflect): ErrPointerRequired, ErrEventTypesDiffer, ErrMisconstructedEvent.
//
// Для go1.18+ есть версия функции на дженериках, в которой вместо паник проблемы с неправильным вызовом определяются
// в момент компиляции/редактирования в IDE.
//
// Design notes - почему отдельная функция, а не метод Event?
//
// Для рефлективной версии пришлось бы возвращать Handler, и кастить к своему типу вручную. Для версии на дженериках
// для этого потребуется сделать тип Event шаблонным, чего делать не хотелось бы (как для совместимости с рефлективной версией,
// так и по другим соображениям). Возвращать же тип-параметр для "нешаблонного" ресивера невозможно из-за текущих (1.18) ограничений дженериков.
func Next(evPtr interface{}) bool {
	dst := reflect.ValueOf(evPtr)
	if dst.Kind() != reflect.Ptr {
		panic(ErrPointerRequired)
	}

	dst = dst.Elem()

	next, ok := dst.Interface().(Handler)
	if !ok {
		panic(ErrPointerRequired)
	}

	next = next.GetEvent().next
	if next == nil {
		return false
	}

	if isTypedNil(next) {
		panic(ErrMisconstructedEvent)
	}

	src := reflect.ValueOf(next) // всегда указатель (так заполняется в eventListFromItems)
	if src.Type() != dst.Type() {
		panic(fmt.Errorf("%w: %v vs %v", ErrEventTypesDiffer, src.Type(), dst.Type()))
	}

	dst.Set(src)

	nextEv := next.GetEvent()
	nextEv.flags |= evIsProcessed

	nextEv.list.queue.logger.Debug().Uint16("queue_id", nextEv.QueueID()).Uint64("event_id", nextEv.id).Interface("event", evPtr).Msg("new event received (batch mode)")

	return true
}

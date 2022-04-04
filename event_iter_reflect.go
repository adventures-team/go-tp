//go:build !go1.18 || gomarkdoc
// +build !go1.18 gomarkdoc

package tp

import (
	"context"
	"fmt"
	"reflect"
)

// Next - итерация по событиям одной версии, полученной в одной пачке.
//
// Для высоконагруженных очередей может быть неплохой выигрыш от мульти-обработки событий: мы можем запросить
// объекты по ID из коробки/базы одним запросом, более того, если набор ID небольшой (например, все юзеры смотрят
// одну какую-то очень популярную страницу), экономия трафика/нагрузки на БД будет существенна.
//
//	func (ev *MyEvent) Handler(ctx context.Context) ... {
//		...
//		ids := make([]uint32, 0, ev.BatchLen())
//
//		for nctx, nev := ctx, ev; tp.Next(&nctx, &ev); {
//			zerolog.Ctx(nctx).Trace().Uint32("user_id", nev.UserID).Msg("fetching user info")
//			ids = append(ids, nev.UserID)
//		}
//
//		... SelectMultiByUserID(ids)
//		// заполняем map по UserID
//
//		for {
//			zerolog.Ctx(ctx).Trace().Uint32("user_id", ev.UserID).Msg("processing user info")
//			Process(byUserID[ev.UserID])
//			ev.Done() // вызывать Done при использовании Next обязательно
//			if !tp.Next(&ctx, &ev) {
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
// Передавать надо указатель на receiver метода Handler, проверки при компиляции нет (в go.18+ будет).
//
// Логгировать надо через явное получение логгера из контекста при каждой итерации цикла. Можно было бы
// заставить юзера вручную логгировать event_id, но это чревато очепятками (строки же не проверяются ничем),
// что может затруднить автоматическую обработку логов.
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
func Next(ctxPtr *context.Context, evPtr interface{}) bool {
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

	nextEventLogger(ctxPtr, nextEv.id).Debug().Uint16("queue_id", nextEv.QueueID()).Uint64("event_id", nextEv.id).Interface("event", evPtr).Msg("new event received (batch mode)")

	return true
}

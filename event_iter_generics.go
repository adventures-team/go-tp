//go:build go1.18 && !golangci
// +build go1.18,!golangci

package tp

import "context"

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
// Вызывать метод Done для событий, полученных при помощи Next, обязательно.
//
// Логгировать надо через явное получение логгера из контекста при каждой итерации цикла. Можно было бы
// заставить юзера вручную логгировать event_id, но это чревато очепятками (строки же не проверяются ничем),
// что может затруднить автоматическую обработку логов.
//
// Версия функции для go1.18+ не кидает паники - ErrPointerRequired невозможна (возникнет ошибка при компиляции),
// ErrEventTypesDiffer и ErrMisconstructedEvent менее вероятны, чем в рефлективной версии, но всё-таки возможны
// при внутренних ошибках в tp. Если эти две паники не возникнут ни разу за время эксплуатации рефлективной версии -
// можно их тут не обрабатывать.
func Next[T Handler](ctxPtr *context.Context, evPtr *T) bool {
	next := (*evPtr).GetEvent().next

	if next == nil {
		return false
	}

	*evPtr = next.(T)
	nextEv := (*evPtr).GetEvent()
	nextEv.flags |= evIsProcessed

	nextEventLogger(ctxPtr, nextEv.EventID).Debug().Interface("event", *evPtr).Msg("new event received (batch mode)")

	return true
}

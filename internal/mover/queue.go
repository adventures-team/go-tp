package mover

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	advslog "github.com/adventures-team/go-adv-slog"
	"github.com/adventures-team/go-queuedproto"

	"github.com/adventures-team/go-tp/queuedclient"
)

// panicDelay - пауза перед продолжением цикла воркера после перехваченной паники.
const panicDelay = 100 * time.Millisecond

// outcome - результат публикации одного события в B.
type outcome int

const (
	outcomeMoved     outcome = iota // опубликовано в B - удалить из A
	outcomeDuplicate                // в B уже есть это же событие (тот же ID и тело) - удалить из A
	outcomeCollision                // в B под этим ID лежит другое событие - оставить в A
	outcomeFailed                   // опубликовать не удалось - оставить в A
)

// queueCounters - счётчики очереди (суммарно по её воркерам) для строки прогресса и итоговой сводки.
type queueCounters struct {
	moved        atomic.Uint64 // перенесено
	duplicates   atomic.Uint64 // уже было в B (тот же ID и тело) - удалено из A
	collisions   atomic.Uint64 // коллизии ID в B - оставлены в A
	failed       atomic.Uint64 // ошибки публикации в B - отложены в A
	deleteErrors atomic.Uint64 // неудачные попытки удаления из A
	deferErrors  atomic.Uint64 // неудачные попытки откладывания в A
}

// counterSnapshot - значения счётчиков очереди в какой-то момент.
type counterSnapshot struct {
	moved, duplicates, collisions, failed, deleteErrors, deferErrors uint64
}

// snapshot снимает значения счётчиков.
func (c *queueCounters) snapshot() counterSnapshot {
	return counterSnapshot{
		moved:        c.moved.Load(),
		duplicates:   c.duplicates.Load(),
		collisions:   c.collisions.Load(),
		failed:       c.failed.Load(),
		deleteErrors: c.deleteErrors.Load(),
		deferErrors:  c.deferErrors.Load(),
	}
}

// attrs возвращает счётчики как атрибуты для лога.
func (c *queueCounters) attrs() []any {
	return c.snapshot().attrs()
}

// delta возвращает прирост счётчиков относительно prev.
func (s counterSnapshot) delta(prev counterSnapshot) counterSnapshot {
	return counterSnapshot{
		moved:        s.moved - prev.moved,
		duplicates:   s.duplicates - prev.duplicates,
		collisions:   s.collisions - prev.collisions,
		failed:       s.failed - prev.failed,
		deleteErrors: s.deleteErrors - prev.deleteErrors,
		deferErrors:  s.deferErrors - prev.deferErrors,
	}
}

// attrs возвращает значения как атрибуты для лога.
func (s counterSnapshot) attrs() []any {
	return []any{
		slog.Uint64("moved", s.moved),
		slog.Uint64("duplicates", s.duplicates),
		slog.Uint64("collisions", s.collisions),
		slog.Uint64("failed", s.failed),
		slog.Uint64("delete_errors", s.deleteErrors),
		slog.Uint64("defer_errors", s.deferErrors),
	}
}

// publishOptions - параметры публикации, прочитанные из конфига один раз на пачку.
type publishOptions struct {
	preserveIDs  bool          // сохранять ID: публиковать с исходным ID в шарде по CalcShard
	initialDelay time.Duration // задержка активации в B
}

// queueWorker - воркер очереди: цикл "выбрать из A - опубликовать в B - удалить из A".
type queueWorker struct {
	conf     *settings
	src, dst Client
	queueID  uint16
	logger   *slog.Logger
	counters *queueCounters

	pendingDelete []queuedproto.EventID // перенесённые, но ещё не удалённые из A события (Delete не прошёл)
	pendingDefer  []queuedproto.EventID // неперенесённые, но ещё не отложенные в A события (Defer не прошёл)
}

// run крутит цикл до отмены ctx (остановка) либо, в режиме one-shot, до опустошения очереди.
// Возвращает true, если очередь пуста (one-shot).
//
// Операции с queued выполняются с контекстом, не зависящим от отмены ctx: остановка не
// прерывает итерацию посередине, каждое выбранное событие удаляется или откладывается.
func (w *queueWorker) run(ctx context.Context) (empty bool) {
	opCtx := advslog.NewContext(context.WithoutCancel(ctx), w.logger)

	for ctx.Err() == nil {
		if w.iterate(ctx, opCtx) {
			return true
		}
	}

	w.finishPending(opCtx) // последняя попытка освободить выбранные события

	return false
}

// iterate - одна итерация цикла. Возвращает true, если в режиме one-shot очередь пуста.
func (w *queueWorker) iterate(ctx, opCtx context.Context) (empty bool) {
	defer func() {
		if reason := recover(); reason != nil {
			advslog.LogPanic(opCtx, "panic in queue worker", reason)
			w.sleep(ctx, panicDelay)
		}
	}()

	if !w.finishPending(opCtx) {
		w.sleep(ctx, w.conf.pollDelay(opCtx))

		return false
	}

	items := w.fetch(opCtx)
	if len(items) == 0 {
		if w.conf.oneShot() && w.queueEmpty(opCtx) {
			return true
		}

		w.sleep(ctx, w.conf.pollDelay(opCtx))

		return false
	}

	moved, failed := w.publishAll(opCtx, items)

	w.pendingDelete = append(w.pendingDelete, moved...)
	w.pendingDefer = append(w.pendingDefer, failed...)
	w.finishPending(opCtx)

	if len(failed) > 0 {
		w.sleep(ctx, w.conf.pollDelay(opCtx)) // не молотить вхолостую, пока B недоступен
	}

	return false
}

// fetch получает активные события из A. Ошибки шардов - в лог (отсутствие очереди в шарде -
// не ошибка), данные с ответивших шардов обрабатываются.
func (w *queueWorker) fetch(ctx context.Context) []queuedproto.QueueItem {
	items, err := w.src.GetActive(ctx, w.queueID, w.conf.fetchBatchSize(ctx))
	if err != nil {
		for _, shardErr := range shardErrors(err) {
			if !errors.Is(shardErr, queuedproto.ErrUnknownQueueType) {
				w.logger.Error("error getting active events", advslog.Err(shardErr))
			}
		}
	}

	if len(items.Items) > 0 && w.logger.Enabled(ctx, slog.LevelDebug) {
		w.logger.Debug("events received", "events_count", len(items.Items), "event_ids", eventIDs(items.Items))
	}

	return items.Items
}

// queueEmpty проверяет по статистике A, что в очереди не осталось событий - ни активных,
// ни отложенных, ни заблокированных. Ошибка запроса - "не пусто".
func (w *queueWorker) queueEmpty(ctx context.Context) bool {
	items, _, locked, err := queueStatSum(ctx, w.src, w.queueID)
	if err != nil {
		w.logger.Error("error getting queue stat", advslog.Err(err))

		return false
	}

	if items != 0 {
		w.logger.Debug("waiting for the remaining events", "items", items, "locked", locked)

		return false
	}

	return true
}

// publishAll публикует события пачки в B. Возвращает события, которые надо удалить
// из A (перенесённые и дубли), и события, которые надо отложить в A (ошибки и коллизии).
func (w *queueWorker) publishAll(ctx context.Context, items []queuedproto.QueueItem) (moved, failed []queuedproto.EventID) {
	var (
		collisions []queuedproto.EventID
		errored    []queuedproto.EventID
		lastErr    error
	)

	opts := publishOptions{
		preserveIDs:  w.conf.preserveIDs(),
		initialDelay: w.conf.initialDelay(ctx),
	}

	for _, item := range items {
		dstID, result, err := w.publishOne(ctx, item, opts)

		switch result {
		case outcomeDuplicate:
			w.counters.duplicates.Add(1)
			w.logger.Info("event is already in the destination, deleting from the source", "event_id", item.EventID, "dst_event_id", dstID)

			moved = append(moved, item.EventID)

		case outcomeCollision:
			w.counters.collisions.Add(1)

			if err != nil {
				lastErr = err
			}

			collisions = append(collisions, item.EventID)
			failed = append(failed, item.EventID)

		case outcomeFailed:
			w.counters.failed.Add(1)

			lastErr = err

			errored = append(errored, item.EventID)
			failed = append(failed, item.EventID)

		default: // outcomeMoved
			w.counters.moved.Add(1)
			w.logger.Debug("event moved", "event_id", item.EventID, "dst_event_id", dstID)

			moved = append(moved, item.EventID)
		}
	}

	if len(collisions) > 0 {
		w.logger.Error("event id collisions in the destination: another event has the same id, keeping the events in the source",
			"events_count", len(collisions), "event_ids", collisions, advslog.Err(lastErr))
	}

	if len(errored) > 0 {
		w.logger.Error("publish failed, deferring the events in the source",
			"events_count", len(errored), "event_ids", errored, advslog.Err(lastErr))
	}

	return moved, failed
}

// publishOne публикует одно событие в B: с новым ID (шард по round robin, ID назначает queued)
// либо с исходным ID в шарде по [queuedclient.CalcShard] - как tp.Queue.Publish для событий
// с явным ID. Ответ "wrong item" при сохранении ID разбирается в [queueWorker.resolveWrongItem].
func (w *queueWorker) publishOne(ctx context.Context, item queuedproto.QueueItem, opts publishOptions) (queuedproto.EventID, outcome, error) {
	eventID := queuedproto.EventID{Shard: queuedclient.BalanceShard}

	if opts.preserveIDs {
		eventID = queuedproto.EventID{
			ID:    item.EventID.ID,
			Shard: queuedclient.CalcShard(item.EventID.ID, w.dst.ShardNum()),
		}
	}

	dstID, err := w.dst.Publish(ctx, w.queueID, eventID, 0, item.Data, queuedclient.PublishInitialDelay(opts.initialDelay))

	switch {
	case err == nil:
		return dstID, outcomeMoved, nil

	case opts.preserveIDs && errors.Is(err, queuedproto.ErrWrongItem):
		result, err := w.resolveWrongItem(ctx, eventID, item.Data)

		return eventID, result, err

	default:
		return queuedproto.EventID{}, outcomeFailed, err
	}
}

// resolveWrongItem разбирает ответ "wrong item" (в этом шарде B такой ID уже есть): если тело
// события в B совпадает с переносимым - это то же событие (дубль, например после падения между
// публикацией и удалением), иначе - коллизия ID. Событие, которое не удалось получить, считается
// коллизией: потеря данных исключена, повтор произойдёт после отложенной активации.
func (w *queueWorker) resolveWrongItem(ctx context.Context, eventID queuedproto.EventID, data []byte) (outcome, error) {
	existing, err := w.dst.GetByIDs(ctx, w.queueID, []queuedproto.EventID{eventID})
	if err != nil {
		return outcomeCollision, err
	}

	if len(existing.Items) != 1 {
		return outcomeCollision, nil
	}

	if bytes.Equal(existing.Items[0].Data, data) {
		return outcomeDuplicate, nil
	}

	return outcomeCollision, nil
}

// finishPending выполняет незавершённые Delete/Defer в A. Возвращает false, если что-то
// не удалось и будет повторено: до успеха воркер не делает новых выборок, иначе по истечении
// таймаута блокировки queued переактивировал бы уже перенесённые события.
func (w *queueWorker) finishPending(ctx context.Context) bool {
	ok := true

	if len(w.pendingDelete) > 0 {
		err := w.src.Delete(ctx, w.queueID, w.pendingDelete)
		if w.commitFailed(err, "delete", len(w.pendingDelete), &w.counters.deleteErrors) {
			ok = false
		} else {
			w.pendingDelete = nil
		}
	}

	if len(w.pendingDefer) > 0 {
		err := w.src.Defer(ctx, w.queueID, w.conf.deferTime(ctx), w.pendingDefer)
		if w.commitFailed(err, "defer", len(w.pendingDefer), &w.counters.deferErrors) {
			ok = false
		} else {
			w.pendingDefer = nil
		}
	}

	return ok
}

// commitFailed разбирает ошибку Delete/Defer: nil - успех; ошибка, которую нет смысла повторять
// (см. retryable) - в лог, операция считается завершённой (событиями займётся queued);
// иначе - в лог, вернуть true (повторить).
func (w *queueWorker) commitFailed(err error, op string, count int, counter *atomic.Uint64) bool {
	if err == nil {
		return false
	}

	counter.Add(1)

	if !retryable(err) {
		w.logger.Error(op+" failed, giving up on these events", "events_count", count, advslog.Err(err))

		return false
	}

	w.logger.Error(op+" failed, will retry before the next fetch", "events_count", count, advslog.Err(err))

	return true
}

// retryable сообщает, стоит ли повторять Delete/Defer после ошибки: да - для "жёстких"
// (транспортных и серверных) ошибок хотя бы одного шарда; нет - для логических
// ([queuedproto.IsSoftError]) и для [queuedproto.ErrItemLocked] (событие в руках другого
// клиента, queued сам снимет блокировку по таймауту).
func retryable(err error) bool {
	errs := shardErrors(err)
	if len(errs) == 0 {
		return false
	}

	for _, shardErr := range errs {
		if !queuedproto.IsSoftError(shardErr) && !errors.Is(shardErr, queuedproto.ErrItemLocked) {
			return true
		}
	}

	return false
}

// shardErrors разворачивает мультишардовую ошибку ([queuedclient.ErrMulti]) в ошибки шардов;
// прочие ошибки возвращаются как есть одним элементом, nil - пустой список.
func shardErrors(err error) []error {
	if err == nil {
		return nil
	}

	var errs queuedclient.ErrMulti

	if errors.As(err, &errs) {
		return errs
	}

	return []error{err}
}

// sleep ждёт delay или отмены ctx.
func (*queueWorker) sleep(ctx context.Context, delay time.Duration) {
	if delay <= 0 {
		return
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// queueStatSum суммирует статистику очереди по шардам.
func queueStatSum(ctx context.Context, client Client, queueID uint16) (items, active, locked uint64, err error) {
	stats, err := client.QueueStat(ctx, queueID)
	if err != nil {
		return 0, 0, 0, err
	}

	for _, stat := range stats {
		items += uint64(stat.ItemsCount)
		active += uint64(stat.ActiveCount)
		locked += uint64(stat.LockedCount)
	}

	return items, active, locked, nil
}

// eventIDs возвращает ID событий пачки (для лога).
func eventIDs(items []queuedproto.QueueItem) []queuedproto.EventID {
	ids := make([]queuedproto.EventID, len(items))

	for i, item := range items {
		ids[i] = item.EventID
	}

	return ids
}

package mover

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	advslog "github.com/adventures-team/go-adv-slog"
	"github.com/adventures-team/go-queuedproto"
	"go.uber.org/mock/gomock"

	"github.com/adventures-team/go-tp/queuedclient"
	mockmover "github.com/adventures-team/go-tp/test/mock/mover"
)

const (
	testQueueID   = uint16(1122)
	testDeferTime = 30 * time.Second
	testShardNum  = uint(2)
)

var errTransport = errors.New("i/o timeout")

// testWorkerConf - настройки воркера для тестов: быстрые паузы, defer_time testDeferTime.
func testWorkerConf(extra map[string]string) *fakeConf {
	values := map[string]string{
		ConfQueueIDs:  fmt.Sprint(testQueueID),
		ConfPollDelay: "0.001",
		ConfDeferTime: "30s",
	}

	for path, value := range extra {
		values[path] = value
	}

	return newFakeConf(values)
}

type testWorker struct {
	*queueWorker
	src, dst *mockmover.MockClient
}

func newTestWorker(t *testing.T, conf *fakeConf) (context.Context, testWorker) {
	t.Helper()

	ctrl := gomock.NewController(t)
	logger := advslog.InitTest(t)

	w := testWorker{
		src: mockmover.NewMockClient(ctrl),
		dst: mockmover.NewMockClient(ctrl),
	}

	w.queueWorker = &queueWorker{
		conf:     newSettings(conf),
		src:      w.src,
		dst:      w.dst,
		queueID:  testQueueID,
		logger:   logger,
		counters: &queueCounters{},
	}

	return advslog.NewContext(context.Background(), logger), w
}

func item(shard uint, id uint64, data string) queuedproto.QueueItem {
	return queuedproto.QueueItem{EventID: queuedproto.EventID{Shard: shard, ID: id}, Data: []byte(data)}
}

func itemList(items ...queuedproto.QueueItem) queuedproto.ItemList {
	return queuedproto.ItemList{Items: items}
}

func ids(items ...queuedproto.QueueItem) []queuedproto.EventID {
	return eventIDs(items)
}

// hardError - ошибка Delete/Defer в том виде, в каком её возвращает queuedclient: транспортная
// ошибка шарда внутри ErrMulti.
func hardError(op string) error {
	return fmt.Errorf("%s: queue_id=%d: %w", op, testQueueID, queuedclient.ErrMulti{
		fmt.Errorf("shard #0 (queued0:20000), event_ids=[1]: %w", errTransport),
	})
}

func softError(op string, err error) error {
	return fmt.Errorf("%s: queue_id=%d: %w", op, testQueueID, queuedclient.ErrMulti{
		fmt.Errorf("shard #0 (queued0:20000), event_ids=[1]: %w", err),
	})
}

func TestIterateNewIDs(t *testing.T) {
	ctx, w := newTestWorker(t, testWorkerConf(nil))
	a, b := item(0, 7, "a"), item(1, 7, "b")
	newID := queuedproto.EventID{Shard: queuedclient.BalanceShard}

	w.src.EXPECT().GetActive(gomock.Any(), testQueueID, DefaultFetchBatchSize).Return(itemList(a, b), nil)
	w.dst.EXPECT().Publish(gomock.Any(), testQueueID, newID, uint32(0), a.Data, gomock.Any()).Return(queuedproto.EventID{Shard: 1, ID: 100}, nil)
	w.dst.EXPECT().Publish(gomock.Any(), testQueueID, newID, uint32(0), b.Data, gomock.Any()).Return(queuedproto.EventID{Shard: 0, ID: 101}, nil)
	w.src.EXPECT().Delete(gomock.Any(), testQueueID, ids(a, b)).Return(nil)

	if empty := w.iterate(ctx, ctx); empty {
		t.Error("iterate returned empty")
	}

	if got := w.counters.moved.Load(); got != 2 {
		t.Errorf("moved = %d, want 2", got)
	}

	if len(w.pendingDelete) != 0 || len(w.pendingDefer) != 0 {
		t.Errorf("pending after iterate: delete %v, defer %v; want none", w.pendingDelete, w.pendingDefer)
	}
}

func TestIteratePreserveIDs(t *testing.T) {
	ctx, w := newTestWorker(t, testWorkerConf(map[string]string{ConfPreserveIDs: "1", ConfInitialDelay: "5"}))
	a := item(0, 7, "a")
	wantID := queuedproto.EventID{ID: 7, Shard: queuedclient.CalcShard(7, testShardNum)}

	w.dst.EXPECT().ShardNum().Return(testShardNum)
	w.src.EXPECT().GetActive(gomock.Any(), testQueueID, DefaultFetchBatchSize).Return(itemList(a), nil)
	w.dst.EXPECT().Publish(gomock.Any(), testQueueID, wantID, uint32(0), a.Data, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ uint16, eventID queuedproto.EventID, _ uint32, _ []byte, opts ...queuedclient.PublishOption) (queuedproto.EventID, error) {
			if len(opts) != 1 {
				t.Errorf("Publish: %d options, want 1 (initial delay)", len(opts))
			}

			return eventID, nil
		})
	w.src.EXPECT().Delete(gomock.Any(), testQueueID, ids(a)).Return(nil)

	w.iterate(ctx, ctx)

	if got := w.counters.moved.Load(); got != 1 {
		t.Errorf("moved = %d, want 1", got)
	}
}

func TestIterateWrongItem(t *testing.T) {
	a := item(0, 7, "a")
	dstID := queuedproto.EventID{ID: 7, Shard: queuedclient.CalcShard(7, testShardNum)}
	wrongItem := fmt.Errorf("Publish: queue_id=%d: %w", testQueueID, queuedproto.ErrWrongItem)

	tests := []struct {
		name      string
		existing  queuedproto.ItemList
		getErr    error
		duplicate bool
	}{
		{name: "same payload is a duplicate", existing: itemList(queuedproto.QueueItem{EventID: dstID, Data: a.Data}), duplicate: true},
		{name: "other payload is a collision", existing: itemList(queuedproto.QueueItem{EventID: dstID, Data: []byte("other")})},
		{name: "missing event is a collision", existing: itemList()},
		{name: "fetch error is a collision", getErr: errTransport},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, w := newTestWorker(t, testWorkerConf(map[string]string{ConfPreserveIDs: "1"}))

			w.dst.EXPECT().ShardNum().Return(testShardNum)
			w.src.EXPECT().GetActive(gomock.Any(), testQueueID, DefaultFetchBatchSize).Return(itemList(a), nil)
			w.dst.EXPECT().Publish(gomock.Any(), testQueueID, dstID, uint32(0), a.Data, gomock.Any()).Return(queuedproto.EventID{}, wrongItem)
			w.dst.EXPECT().GetByIDs(gomock.Any(), testQueueID, []queuedproto.EventID{dstID}).Return(tt.existing, tt.getErr)

			if tt.duplicate {
				w.src.EXPECT().Delete(gomock.Any(), testQueueID, ids(a)).Return(nil)
			} else {
				w.src.EXPECT().Defer(gomock.Any(), testQueueID, testDeferTime, ids(a)).Return(nil)
			}

			w.iterate(ctx, ctx)

			duplicates, collisions := w.counters.duplicates.Load(), w.counters.collisions.Load()

			if tt.duplicate && (duplicates != 1 || collisions != 0) || !tt.duplicate && (duplicates != 0 || collisions != 1) {
				t.Errorf("duplicates = %d, collisions = %d (duplicate case: %v)", duplicates, collisions, tt.duplicate)
			}
		})
	}
}

// Неперенесённые события откладываются в A, перенесённые - удаляются; после ошибок публикации
// воркер делает паузу.
func TestIteratePublishFailure(t *testing.T) {
	ctx, w := newTestWorker(t, testWorkerConf(map[string]string{ConfPollDelay: "0.05"}))
	a, b := item(0, 1, "a"), item(0, 2, "b")
	newID := queuedproto.EventID{Shard: queuedclient.BalanceShard}

	w.src.EXPECT().GetActive(gomock.Any(), testQueueID, DefaultFetchBatchSize).Return(itemList(a, b), nil)
	w.dst.EXPECT().Publish(gomock.Any(), testQueueID, newID, uint32(0), a.Data, gomock.Any()).Return(queuedproto.EventID{ID: 10}, nil)
	w.dst.EXPECT().Publish(gomock.Any(), testQueueID, newID, uint32(0), b.Data, gomock.Any()).Return(queuedproto.EventID{}, errTransport)
	w.src.EXPECT().Delete(gomock.Any(), testQueueID, ids(a)).Return(nil)
	w.src.EXPECT().Defer(gomock.Any(), testQueueID, testDeferTime, ids(b)).Return(nil)

	started := time.Now()

	w.iterate(ctx, ctx)

	if elapsed := time.Since(started); elapsed < 50*time.Millisecond {
		t.Errorf("no pause after publish failures: iterate took %v", elapsed)
	}

	if moved, failed := w.counters.moved.Load(), w.counters.failed.Load(); moved != 1 || failed != 1 {
		t.Errorf("moved = %d, failed = %d; want 1, 1", moved, failed)
	}
}

// Неудавшийся Delete повторяется в начале следующей итерации, до успеха новых выборок нет.
func TestIteratePendingDeleteRetried(t *testing.T) {
	ctx, w := newTestWorker(t, testWorkerConf(nil))
	a := item(0, 1, "a")
	newID := queuedproto.EventID{Shard: queuedclient.BalanceShard}

	gomock.InOrder(
		w.src.EXPECT().GetActive(gomock.Any(), testQueueID, DefaultFetchBatchSize).Return(itemList(a), nil),
		w.dst.EXPECT().Publish(gomock.Any(), testQueueID, newID, uint32(0), a.Data, gomock.Any()).Return(queuedproto.EventID{ID: 10}, nil),
		w.src.EXPECT().Delete(gomock.Any(), testQueueID, ids(a)).Return(hardError("Delete")),
		w.src.EXPECT().Delete(gomock.Any(), testQueueID, ids(a)).Return(hardError("Delete")),
		w.src.EXPECT().Delete(gomock.Any(), testQueueID, ids(a)).Return(nil),
		w.src.EXPECT().GetActive(gomock.Any(), testQueueID, DefaultFetchBatchSize).Return(itemList(), nil),
	)

	w.iterate(ctx, ctx) // выборка, публикация, неудачный Delete

	if len(w.pendingDelete) != 1 {
		t.Fatalf("pendingDelete = %v, want the moved event", w.pendingDelete)
	}

	w.iterate(ctx, ctx) // повтор Delete, снова неудачно - без выборки
	w.iterate(ctx, ctx) // повтор Delete успешен, затем выборка

	if len(w.pendingDelete) != 0 {
		t.Errorf("pendingDelete = %v after a successful retry, want none", w.pendingDelete)
	}

	if got := w.counters.deleteErrors.Load(); got != 2 {
		t.Errorf("delete_errors = %d, want 2", got)
	}
}

// Логические ошибки и "item locked" не повторяются: событием займётся queued.
func TestIterateCommitErrorsNotRetried(t *testing.T) {
	for _, err := range []error{queuedproto.ErrItemLocked, queuedproto.ErrWrongRequest} {
		t.Run(err.Error(), func(t *testing.T) {
			ctx, w := newTestWorker(t, testWorkerConf(nil))
			a, b := item(0, 1, "a"), item(0, 2, "b")
			newID := queuedproto.EventID{Shard: queuedclient.BalanceShard}

			w.src.EXPECT().GetActive(gomock.Any(), testQueueID, DefaultFetchBatchSize).Return(itemList(a, b), nil)
			w.dst.EXPECT().Publish(gomock.Any(), testQueueID, newID, uint32(0), a.Data, gomock.Any()).Return(queuedproto.EventID{ID: 10}, nil)
			w.dst.EXPECT().Publish(gomock.Any(), testQueueID, newID, uint32(0), b.Data, gomock.Any()).Return(queuedproto.EventID{}, errTransport)
			w.src.EXPECT().Delete(gomock.Any(), testQueueID, ids(a)).Return(softError("Delete", err))
			w.src.EXPECT().Defer(gomock.Any(), testQueueID, testDeferTime, ids(b)).Return(softError("Defer", err))

			w.iterate(ctx, ctx)

			if len(w.pendingDelete) != 0 || len(w.pendingDefer) != 0 {
				t.Errorf("pending after a non-retryable error: delete %v, defer %v; want none", w.pendingDelete, w.pendingDefer)
			}

			if del, def := w.counters.deleteErrors.Load(), w.counters.deferErrors.Load(); del != 1 || def != 1 {
				t.Errorf("delete_errors = %d, defer_errors = %d; want 1, 1", del, def)
			}
		})
	}
}

// Ошибки части шардов при выборке не мешают обработать события с остальных; отсутствие
// очереди в шарде - не ошибка.
func TestIterateFetchErrors(t *testing.T) {
	ctx, w := newTestWorker(t, testWorkerConf(nil))
	a := item(1, 1, "a")
	newID := queuedproto.EventID{Shard: queuedclient.BalanceShard}
	partial := fmt.Errorf("GetActive: queue_id=%d: %w", testQueueID, queuedclient.ErrMulti{
		fmt.Errorf("shard #0 (queued0:20000): %w", queuedproto.ErrUnknownQueueType),
	})

	w.src.EXPECT().GetActive(gomock.Any(), testQueueID, DefaultFetchBatchSize).Return(itemList(a), partial)
	w.dst.EXPECT().Publish(gomock.Any(), testQueueID, newID, uint32(0), a.Data, gomock.Any()).Return(queuedproto.EventID{ID: 10}, nil)
	w.src.EXPECT().Delete(gomock.Any(), testQueueID, ids(a)).Return(nil)

	w.iterate(ctx, ctx)

	if got := w.counters.moved.Load(); got != 1 {
		t.Errorf("moved = %d, want 1", got)
	}
}

func TestIterateOneShot(t *testing.T) {
	tests := []struct {
		name  string
		stats []queuedproto.RespQueueStat
		err   error
		empty bool
	}{
		{name: "no events", stats: []queuedproto.RespQueueStat{{}, {}}, empty: true},
		{name: "deferred events remain", stats: []queuedproto.RespQueueStat{{}, {ItemsCount: 3}}},
		{name: "stat error", err: errTransport},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, w := newTestWorker(t, testWorkerConf(map[string]string{ConfOneShot: "1"}))

			w.src.EXPECT().GetActive(gomock.Any(), testQueueID, DefaultFetchBatchSize).Return(itemList(), nil)
			w.src.EXPECT().QueueStat(gomock.Any(), testQueueID).Return(tt.stats, tt.err)

			if empty := w.iterate(ctx, ctx); empty != tt.empty {
				t.Errorf("iterate returned empty = %v, want %v", empty, tt.empty)
			}
		})
	}
}

// В режиме сервиса пустая выборка - пауза, статистика не запрашивается.
func TestIterateServiceIdle(t *testing.T) {
	ctx, w := newTestWorker(t, testWorkerConf(nil))

	w.src.EXPECT().GetActive(gomock.Any(), testQueueID, DefaultFetchBatchSize).Return(itemList(), nil)

	if empty := w.iterate(ctx, ctx); empty {
		t.Error("iterate returned empty in service mode")
	}
}

// Остановка: цикл завершается, незавершённый Delete пробуется ещё раз.
func TestRunStopFinishesPending(t *testing.T) {
	ctx, w := newTestWorker(t, testWorkerConf(nil))
	a := item(0, 1, "a")

	w.pendingDelete = ids(a)
	w.src.EXPECT().Delete(gomock.Any(), testQueueID, ids(a)).Return(nil)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	if empty := w.run(cancelled); empty {
		t.Error("run returned empty after a stop")
	}

	if len(w.pendingDelete) != 0 {
		t.Errorf("pendingDelete = %v after stop, want none", w.pendingDelete)
	}
}

func TestIteratePanicRecovered(t *testing.T) {
	ctx, w := newTestWorker(t, testWorkerConf(nil))

	gomock.InOrder(
		w.src.EXPECT().GetActive(gomock.Any(), testQueueID, DefaultFetchBatchSize).
			DoAndReturn(func(context.Context, uint16, int) (queuedproto.ItemList, error) { panic("boom") }),
		w.src.EXPECT().GetActive(gomock.Any(), testQueueID, DefaultFetchBatchSize).Return(itemList(), nil),
	)

	w.iterate(ctx, ctx) // паника перехвачена
	w.iterate(ctx, ctx) // цикл продолжается
}

func TestRetryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "plain transport error", err: errTransport, want: true},
		{name: "hard shard error", err: hardError("Delete"), want: true},
		{name: "item locked", err: softError("Delete", queuedproto.ErrItemLocked), want: false},
		{name: "logical error", err: softError("Delete", queuedproto.ErrWrongRequest), want: false},
		{name: "mixed shards", err: queuedclient.ErrMulti{
			fmt.Errorf("shard #0: %w", queuedproto.ErrItemLocked),
			fmt.Errorf("shard #1: %w", errTransport),
		}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := retryable(tt.err); got != tt.want {
				t.Errorf("retryable(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

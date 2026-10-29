package mover

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	advslog "github.com/adventures-team/go-adv-slog"
	"github.com/adventures-team/go-queuedproto"
	"go.uber.org/mock/gomock"

	"github.com/adventures-team/go-tp/queuedclient"
	mockmover "github.com/adventures-team/go-tp/test/mock/mover"
)

const testTimeout = 10 * time.Second

type testMover struct {
	*Mover
	conf     *fakeConf
	src, dst *mockmover.MockClient
}

func newTestMover(t *testing.T, values map[string]string) testMover {
	t.Helper()

	ctrl := gomock.NewController(t)
	conf := testWorkerConf(values)

	m := testMover{
		conf: conf,
		src:  mockmover.NewMockClient(ctrl),
		dst:  mockmover.NewMockClient(ctrl),
	}
	m.Mover = New(conf, m.src, m.dst, advslog.InitTest(t))

	return m
}

func zeroStats() []queuedproto.RespQueueStat {
	return []queuedproto.RespQueueStat{{}, {}}
}

// One-shot: события выбираются, переносятся, удаляются; Run выходит, когда статистика A пуста.
func TestRunOneShot(t *testing.T) {
	m := newTestMover(t, map[string]string{ConfQueueIDs: "1,2", ConfOneShot: "1", ConfWorkers: "2"})
	newID := queuedproto.EventID{Shard: queuedclient.BalanceShard}

	var fetched atomic.Int32 // по одной пачке на очередь, затем пусто

	for _, queueID := range []uint16{1, 2} {
		m.src.EXPECT().GetActive(gomock.Any(), queueID, DefaultFetchBatchSize).
			DoAndReturn(func(context.Context, uint16, int) (queuedproto.ItemList, error) {
				if fetched.Add(1) <= 2 {
					return itemList(item(0, uint64(queueID), "a")), nil
				}

				return itemList(), nil
			}).AnyTimes()
		m.dst.EXPECT().Publish(gomock.Any(), queueID, newID, uint32(0), []byte("a"), gomock.Any()).
			Return(queuedproto.EventID{ID: 100}, nil).AnyTimes()
		m.src.EXPECT().Delete(gomock.Any(), queueID, []queuedproto.EventID{{ID: uint64(queueID)}}).Return(nil).AnyTimes()
		m.src.EXPECT().QueueStat(gomock.Any(), queueID).Return(zeroStats(), nil).AnyTimes()
	}

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	if err := m.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	for _, queueID := range []uint16{1, 2} {
		qs := m.queues[queueID]
		if qs == nil || !qs.done {
			t.Errorf("queue %d: not drained", queueID)
		}
	}

	if moved := m.queues[1].counters.moved.Load() + m.queues[2].counters.moved.Load(); moved != 2 {
		t.Errorf("moved = %d, want 2", moved)
	}
}

// One-shot прерван до опустошения (в A остаются отложенные события) - ErrInterrupted.
func TestRunInterrupted(t *testing.T) {
	m := newTestMover(t, map[string]string{ConfQueueIDs: "1", ConfOneShot: "1"})
	remaining := []queuedproto.RespQueueStat{{ItemsCount: 1}, {}}
	polled := make(chan struct{}, 1)

	m.src.EXPECT().GetActive(gomock.Any(), uint16(1), DefaultFetchBatchSize).Return(itemList(), nil).AnyTimes()
	m.src.EXPECT().QueueStat(gomock.Any(), uint16(1)).DoAndReturn(func(context.Context, uint16) ([]queuedproto.RespQueueStat, error) {
		select {
		case polled <- struct{}{}:
		default:
		}

		return remaining, nil
	}).AnyTimes()

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	done := make(chan error, 1)

	go func() { done <- m.Run(ctx) }()

	<-polled // воркер увидел непустую очередь и ждёт
	cancel()

	if err := <-done; !errors.Is(err, ErrInterrupted) {
		t.Errorf("Run: %v, want ErrInterrupted", err)
	}
}

func TestRunEmptyQueueIDsOneShot(t *testing.T) {
	m := newTestMover(t, map[string]string{ConfQueueIDs: "", ConfOneShot: "1"})

	if err := m.Run(context.Background()); err != nil {
		t.Errorf("Run: %v, want nil", err)
	}
}

func TestRunConfigError(t *testing.T) {
	m := newTestMover(t, map[string]string{ConfQueueIDs: "1", ConfWorkers: "many"})

	if err := m.Run(context.Background()); err == nil {
		t.Error("Run: no error for an invalid workers value")
	}
}

// Сервис: смена queue_ids в onlineconf (уведомление подписки) переключает воркеры на новую очередь;
// остановка завершает Run без ошибки.
func TestRunServiceReload(t *testing.T) {
	m := newTestMover(t, map[string]string{ConfQueueIDs: "1"})
	queue2 := make(chan struct{}, 1)

	m.src.EXPECT().GetActive(gomock.Any(), uint16(1), DefaultFetchBatchSize).Return(itemList(), nil).AnyTimes()
	m.src.EXPECT().GetActive(gomock.Any(), uint16(2), DefaultFetchBatchSize).DoAndReturn(func(context.Context, uint16, int) (queuedproto.ItemList, error) {
		select {
		case queue2 <- struct{}{}:
		default:
		}

		return itemList(), nil
	}).AnyTimes()

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	done := make(chan error, 1)

	go func() { done <- m.Run(ctx) }()

	time.Sleep(20 * time.Millisecond)
	m.conf.set(ConfQueueIDs, "2")

	select {
	case <-queue2:
	case <-ctx.Done():
		t.Fatal("queue 2 was not polled after the config change")
	}

	cancel()

	if err := <-done; err != nil {
		t.Errorf("Run: %v, want nil", err)
	}
}

// apply/resize: число воркеров подгоняется под конфиг, исчезнувшие очереди останавливаются.
func TestApplyResize(t *testing.T) {
	m := newTestMover(t, map[string]string{ConfQueueIDs: "1,2", ConfWorkers: "2"})
	ctx := advslog.NewContext(context.Background(), m.logger)

	m.src.EXPECT().GetActive(gomock.Any(), gomock.Any(), DefaultFetchBatchSize).Return(itemList(), nil).AnyTimes()

	check := func(want map[uint16]int) {
		t.Helper()

		if len(m.queues) != len(want) {
			t.Errorf("queues = %v, want %v", m.queueIDs(), want)
		}

		for id, workers := range want {
			if qs := m.queues[id]; qs == nil || len(qs.active) != workers {
				t.Errorf("queue %d: %d active workers, want %d", id, activeWorkers(qs), workers)
			}
		}
	}

	m.apply(ctx)
	check(map[uint16]int{1: 2, 2: 2})

	m.conf.set(ConfWorkers, "1")
	m.apply(ctx)
	check(map[uint16]int{1: 1, 2: 1})

	m.conf.set(ConfQueueIDs, "2,3")
	m.conf.set(ConfWorkers, "3")
	m.apply(ctx)
	check(map[uint16]int{2: 3, 3: 3})

	m.conf.set(ConfQueueIDs, "x")
	m.apply(ctx) // ошибка в queue_ids - прежний набор очередей продолжает работать
	check(map[uint16]int{2: 3, 3: 3})

	m.conf.set(ConfQueueIDs, "")
	m.apply(ctx)
	check(map[uint16]int{})

	if err := m.finish(); err != nil {
		t.Errorf("finish: %v", err)
	}
}

func activeWorkers(qs *queueState) int {
	if qs == nil {
		return 0
	}

	return len(qs.active)
}

// onWorkerExit: очередь опустошена только после перепроверки статистики, иначе воркеры
// перезапускаются после poll_delay.
func TestOnWorkerExit(t *testing.T) {
	m := newTestMover(t, map[string]string{ConfQueueIDs: "1", ConfOneShot: "1"})
	ctx := advslog.NewContext(context.Background(), m.logger)
	qs := &queueState{counters: &queueCounters{}, active: map[int]context.CancelFunc{}, running: 1}
	m.queues[1] = qs

	gomock.InOrder(
		m.src.EXPECT().QueueStat(gomock.Any(), uint16(1)).Return([]queuedproto.RespQueueStat{{ItemsCount: 2, LockedCount: 1}}, nil),
		m.src.EXPECT().QueueStat(gomock.Any(), uint16(1)).Return(zeroStats(), nil),
	)

	if m.onWorkerExit(ctx, workerExit{queueID: 1, id: 1}) {
		t.Error("onWorkerExit: all done after an exit without the empty flag")
	}

	qs.running = 1

	if m.onWorkerExit(ctx, workerExit{queueID: 1, id: 2, empty: true}) || qs.done {
		t.Error("onWorkerExit: queue marked drained while events remain")
	}

	select {
	case <-m.reload: // перезапуск воркеров запланирован
	case <-time.After(time.Second):
		t.Error("no reload scheduled after an incomplete drain")
	}

	qs.running = 1

	if !m.onWorkerExit(ctx, workerExit{queueID: 1, id: 3, empty: true}) || !qs.done {
		t.Error("onWorkerExit: queue not drained with an empty stat")
	}

	if m.onWorkerExit(ctx, workerExit{queueID: 9, id: 4, empty: true}) {
		t.Error("onWorkerExit: exit of an unknown queue reported as all done")
	}
}

func TestQueueStatSum(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mockmover.NewMockClient(ctrl)

	client.EXPECT().QueueStat(gomock.Any(), uint16(1)).Return([]queuedproto.RespQueueStat{
		{ItemsCount: 5, ActiveCount: 2, LockedCount: 1},
		{ItemsCount: 3, ActiveCount: 1, LockedCount: 2},
	}, nil)
	client.EXPECT().QueueStat(gomock.Any(), uint16(2)).Return(nil, errTransport)

	items, active, locked, err := queueStatSum(context.Background(), client, 1)
	if err != nil || items != 8 || active != 3 || locked != 3 {
		t.Errorf("queueStatSum = %d, %d, %d, %v; want 8, 3, 3, nil", items, active, locked, err)
	}

	if _, _, _, err := queueStatSum(context.Background(), client, 2); !errors.Is(err, errTransport) {
		t.Errorf("queueStatSum error = %v, want %v", err, errTransport)
	}
}

func TestCounterSnapshot(t *testing.T) {
	c := &queueCounters{}
	c.moved.Add(5)
	c.failed.Add(2)

	first := c.snapshot()

	c.moved.Add(1)

	delta := c.snapshot().delta(first)
	if delta.moved != 1 || delta.failed != 0 {
		t.Errorf("delta = %+v, want moved 1, failed 0", delta)
	}

	if attrs := c.attrs(); len(attrs) != 6 || fmt.Sprint(attrs[0]) != "moved=6" {
		t.Errorf("attrs = %v", attrs)
	}
}

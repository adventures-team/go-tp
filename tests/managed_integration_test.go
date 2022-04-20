//go:build integration
// +build integration

package tptest

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	zLog "github.com/rs/zerolog/log"

	"gitlab.corp.mail.ru/mygo/queuedproto.git"
	"gitlab.corp.mail.ru/mygo/tp.git"
	"gitlab.corp.mail.ru/mygo/tp.git/queueconfig"
	"gitlab.corp.mail.ru/mygo/utils.git/mylog"
)

// тесты на управление пулом при помощи онлайнконфа (проще говоря - тест системы целиком)

func testManaged(t *testing.T, doTest func(*tp.QueueManager, *counter, <-chan struct{})) *tp.QueueManager {
	doneChan := make(chan struct{})

	defer func() {
		close(doneChan)
		resetConfig()
	}()

	conf := newMockConf(t, testQueueSleeperID)

	qm := tp.Config{
		Config:       conf,
		InstanceName: "test",
		Logger:       mylog.InitTestZerolog(t),
	}.InitProcessor()

	zLog.Info().Int("pid", os.Getpid()).Send()

	cnt := newCounter(0)

	err := qm.Register(testQueueSleeperID, &eventSleeper{}, cnt)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		timer := time.NewTimer(time.Second)

		for {
			zLog.Info().Int32("*** workers_count ***", cnt.Get()).Send()

			select {
			case <-doneChan:
				return

			case <-timer.C:
			}
		}
	}()

	go doTest(qm, cnt, doneChan)
	qm.Manage()

	if workersLeft := cnt.Get(); workersLeft != 0 {
		t.Fatalf("%d workers are still working", workersLeft)
	}

	return qm
}

func reloadConfig() {
	time.Sleep(100 * time.Millisecond) // race condition с запуском менеджера (это не проблема - в проде посылать себе сигналы/закладываться на последовательность выполнения горутин мы точно не должны)

	if err := syscall.Kill(os.Getpid(), tp.SignalReload); err != nil {
		zLog.Err(err).Msg("syscall.Kill")
	}
}

// TODO ускорить этот тест, либо скипнуть с комментарием
func TestDeferred(t *testing.T) {
	ctx := context.Background()

	testManaged(t, func(qm *tp.QueueManager, cnt *counter, _ <-chan struct{}) {
		for i := 0; i < 10; i++ {
			_, err := qm.Publish(ctx, &eventSleeper{
				DurationMin: 3 * time.Second,
				DurationMax: 5 * time.Second,
				Percentage:  40,
			})

			if err != nil {
				t.Fatal("Publish:", err)
			}

			cnt.Add(1)
			time.Sleep(500 * time.Millisecond)
		}

		cnt.Wait()
		qm.Stop()
	})
}

func TestOOM(t *testing.T) {
	ctx := context.Background()
	eventIDs := make([]queuedproto.EventID, 0, 50)
	newEvents := sync.Mutex{}
	confLogLevel = "debug"
	confFetchBatchSize = 1 // чтобы не выгребало пачками - а заняло все воркеры

	qm := testManaged(t, func(qm *tp.QueueManager, cnt *counter, doneChan <-chan struct{}) {
		for i := 0; i < 50; i++ {
			newEvents.Lock()
			eventID, err := qm.Publish(ctx, &eventSleeper{
				DurationMin: 2 * time.Second,
				DurationMax: 5 * time.Second,
				Percentage:  10,
			})

			if err != nil {
				t.Fatal("Publish:", err)
			}

			zLog.Info().Stringer("event_id", eventID).Msg("event published")

			eventIDs = append(eventIDs, eventID)

			cnt.Add(1)
			newEvents.Unlock()
			time.Sleep(100 * time.Millisecond)
		}

		zLog.Warn().Msg("waiting 10s")
		time.Sleep(10 * time.Second)

		zLog.Warn().Msg("dropping the soft limit")
		confOOMSoftLimit.Store("10Kb")

		time.Sleep(5 * time.Second)

		workersGot := cnt.Get()
		workersExpected := int32(float64(atomic.LoadInt32(&confWorkers)) * queueconfig.DefaultDownscaleRate)

		if workersGot > workersExpected {
			t.Fatalf("too many workers when the soft memory limit is reached: got %d, expected: no more than %d", workersGot, workersExpected)
		}

		time.Sleep(5 * time.Second)

		zLog.Warn().Msg("dropping the hard limit")
		confOOMHardLimit.Store("10Kb")

		select {
		case <-time.NewTimer(10 * time.Second).C:
			t.Fatal("must exit when the hard memory limit is reached")

		case <-doneChan:
		}
	})

	newEvents.Lock() // стопим цикл отправки новых сообщений
	zLog.Info().Interface("event_ids", eventIDs).Msg("deleting events")

	if err := qm.Queued().Delete(ctx, testQueueSleeperID, eventIDs); err != nil {
		t.Fatal(err)
	}
}

func TestResize(t *testing.T) {
	ctx := context.Background()
	eventIDs := make([]queuedproto.EventID, 0, 50)
	newEvents := sync.Mutex{}
	confLogLevel = "trace"
	confFetchBatchSize = 1 // чтобы не выгребало пачками - а заняло все воркеры

	qm := testManaged(t, func(qm *tp.QueueManager, cnt *counter, doneChan <-chan struct{}) {
		newEvents.Lock()

		for i := 0; i < 50; i++ {
			eventID, err := qm.Publish(ctx, &eventSleeper{
				DurationMin: 2 * time.Second,
				DurationMax: 5 * time.Second,
				Percentage:  10,
			})

			if err != nil {
				t.Fatal("Publish:", err)
			}

			zLog.Info().Stringer("event_id", eventID).Msg("event published")

			eventIDs = append(eventIDs, eventID)

			cnt.Add(1)
		}

		newEvents.Unlock()

		time.Sleep(10 * time.Second)
		zLog.Warn().Msgf("setting workers_max=%d", workersMaxLimit)
		atomic.StoreInt32(&confWorkersMax, workersMaxLimit)
		reloadConfig()
		time.Sleep(5 * time.Second)

		if workersGot := cnt.Get(); workersGot > workersMaxLimit {
			t.Fatalf("got %d workers when workers_max=%d is set", workersGot, workersMaxLimit)
		}

		zLog.Warn().Msgf("setting workers_max=%d", workersMaxDefault)
		atomic.StoreInt32(&confWorkersMax, workersMaxDefault)
		reloadConfig()
		time.Sleep(5 * time.Second)
		zLog.Warn().Msgf("setting workers_max_per_queue=%d", workersMaxLimit)
		atomic.StoreInt32(&confWorkersMaxPerQueue, workersMaxLimit)
		reloadConfig()
		time.Sleep(5 * time.Second)

		if workersGot := cnt.Get(); workersGot > workersMaxLimit {
			t.Fatalf("got %d workers when workers_max_per_queue=%d is set", workersGot, workersMaxLimit)
		}

		qm.Stop()

		select {
		case <-time.NewTimer(10 * time.Second).C:
			t.Fatal("should exit earlier")

		case <-doneChan:
		}
	})

	newEvents.Lock() // стопим цикл отправки новых сообщений
	zLog.Info().Interface("event_ids", eventIDs).Msg("deleting events")

	if err := qm.Queued().Delete(ctx, testQueueSleeperID, eventIDs); err != nil {
		t.Fatal(err)
	}
}

// TestBatchDelete - тест на race condition при удалении (RcItemLocked)
func TestBatchDelete(t *testing.T) {
	ctx := context.Background()
	eventIDs := make([]queuedproto.EventID, 0, 50)
	newEvents := sync.Mutex{}
	confLogLevel = "trace"
	confFetchBatchSize = 1 // чтобы не выгребало пачками - а заняло все воркеры

	qm := testManaged(t, func(qm *tp.QueueManager, cnt *counter, doneChan <-chan struct{}) {
		newEvents.Lock()

		for i := 0; i < 50; i++ {
			eventID, err := qm.Publish(ctx, &eventSleeper{
				DurationMin: time.Second,
				DurationMax: time.Second,
				Percentage:  0,
			})

			if err != nil {
				t.Fatal("Publish:", err)
			}

			zLog.Info().Stringer("event_id", eventID).Msg("event published")

			eventIDs = append(eventIDs, eventID)

			cnt.Add(1)
		}

		newEvents.Unlock()

		time.Sleep(500 * time.Millisecond)
		qm.Stop()
	})

	newEvents.Lock() // стопим цикл отправки новых сообщений
	zLog.Info().Interface("event_ids", eventIDs).Msg("deleting events")

	if err := qm.Queued().Delete(ctx, testQueueSleeperID, eventIDs); err != nil {
		t.Fatal(err)
	}
}

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

func testManaged(t *testing.T, doTest func(*tp.QueueManager, *counter)) *tp.QueueManager {
	conf := newMockConf(t, testQueueSleeperID)

	qm := tp.Config{
		Config:       conf,
		InstanceName: "test",
		Logger:       mylog.InitTestZerolog(t),
	}.InitProcessor()

	cnt := newCounter(0)

	err := qm.Register(testQueueSleeperID, &eventSleeper{}, cnt)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for {
			zLog.Info().Int32("*** workers_count ***", cnt.Get()).Send()
			time.Sleep(1 * time.Second)
		}
	}()

	go doTest(qm, cnt)
	qm.Manage()

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

	testManaged(t, func(qm *tp.QueueManager, cnt *counter) {
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

	qm := testManaged(t, func(qm *tp.QueueManager, cnt *counter) {
		for i := 0; i < 50; i++ {
			newEvents.Lock()
			eventID, err := qm.Publish(ctx, &eventSleeper{
				DurationMin: 5,
				DurationMax: time.Duration(5.5 * float64(time.Second)),
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

		time.Sleep(10 * time.Second) // если всё ок - не должны дождаться выхода этого слипа - выйдет основная горутина

		t.Fatal("must exit when the hard memory limit is reached")
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
	confLogLevel = "debug"
	confFetchBatchSize = 1 // чтобы не выгребало пачками - а заняло все воркеры

	qm := testManaged(t, func(qm *tp.QueueManager, cnt *counter) {
		newEvents.Lock()

		for i := 0; i < 50; i++ {
			eventID, err := qm.Publish(ctx, &eventSleeper{
				DurationMin: 5,
				DurationMax: time.Duration(5.5 * float64(time.Second)),
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
		time.Sleep(10 * time.Second)
		t.Fatal("should exit earlier")
	})

	newEvents.Lock() // стопим цикл отправки новых сообщений
	zLog.Info().Interface("event_ids", eventIDs).Msg("deleting events")

	if err := qm.Queued().Delete(ctx, testQueueSleeperID, eventIDs); err != nil {
		t.Fatal(err)
	}
}

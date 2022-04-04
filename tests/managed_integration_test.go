//go:build integration
// +build integration

package tptest

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"

	zLog "github.com/rs/zerolog/log"

	"gitlab.corp.mail.ru/mygo/tp.git"
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
			zLog.Info().Int32("workers_count", cnt.Get()).Send()
			time.Sleep(1 * time.Second)
		}
	}()

	go doTest(qm, cnt)
	qm.Manage()

	return qm
}

func reloadConfig() {
	time.Sleep(100 * time.Millisecond) // race condition с запуском менеджера (это не проблема - в проде посылать себе сигналы/закладываться на работу какой-то из горутин мы точно не должны)

	if err := syscall.Kill(os.Getpid(), tp.SignalReload); err != nil {
		zLog.Err(err).Msg("syscall.Kill")
	}
}

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

		go func() {
			cnt.Wait()
			qm.Stop()
		}()
	})
}

func TestOOM(t *testing.T) {
	ctx := context.Background()
	eventIDs := make([]uint64, 0, 50)
	confLogLevel = "info"

	qm := testManaged(t, func(qm *tp.QueueManager, cnt *counter) {
		for i := 0; i < 50; i++ {
			eventID, err := qm.Publish(ctx, &eventSleeper{
				DurationMin: 5,
				DurationMax: time.Duration(5.5 * float64(time.Second)),
				Percentage:  10,
			})

			if err != nil {
				t.Fatal("Publish:", err)
			}

			zLog.Info().Uint64("event_id", eventID).Msg("event published")

			eventIDs = append(eventIDs, eventID)

			cnt.Add(1)
			time.Sleep(500 * time.Millisecond)
		}

		zLog.Warn().Msg("waiting 10s")
		time.Sleep(10 * time.Second)

		confOOMSoftLimit = "10Kb"

		go func() {

		}()
	})

	zLog.Info().Uints64("event_ids", eventIDs).Msg("deleting events")

	if err := qm.Queued().Delete(ctx, testQueueSleeperID, eventIDs); err != nil {
		t.Fatal(err)
	}
}

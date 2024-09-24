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

func testManaged(t *testing.T, doTest func(*tp.StandaloneMaster, *counter, <-chan struct{})) *tp.StandaloneMaster {
	mylog.TestTimeFormat = "15:04:05.000"
	doneChan := make(chan struct{})

	defer func() {
		close(doneChan)
		resetConfig()
	}()

	conf := newMockConf(t, testSleeperQueueID)

	qm := tp.Config{
		Config:       conf,
		InstanceName: "test",
		Logger:       mylog.InitTestZerolog(t),
	}.InitProcessor()

	zLog.Info().Int("pid", os.Getpid()).Send()

	cnt := newCounter(0)
	svc := testService{Counter: cnt}

	err := tp.Register(qm.Queue, svc.HandleSleeper)
	if err != nil {
		zLog.Fatal().Err(err).Msg("Queue.Register")
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
	qm.Run()

	if workersLeft := cnt.Get(); workersLeft != 0 {
		zLog.Fatal().Msgf("%d workers are still working", workersLeft)
	}

	return qm
}

func reloadConfig() {
	time.Sleep(100 * time.Millisecond) // race condition с запуском менеджера (это не проблема - в проде посылать себе сигналы/закладываться на последовательность выполнения горутин мы точно не будем)

	if err := syscall.Kill(os.Getpid(), tp.SignalReload); err != nil {
		zLog.Err(err).Msg("syscall.Kill")
	}
}

// TODO ускорить этот тест, либо скипнуть с комментарием
func TestDeferred(t *testing.T) {
	ctx := context.Background()
	// confLogLevel = logLevelTrace

	testManaged(t, func(qm *tp.StandaloneMaster, cnt *counter, _ <-chan struct{}) {
		for i := 0; i < 10; i++ {
			eventID, err := qm.Publish(ctx, &eventSleeper{
				DurationMin: durationMin,
				DurationMax: durationMax,
				Percentage:  40, // ускорять тест тут )) поставить больше вероятность успешного окончания
			})

			if err != nil {
				zLog.Panic().Err(err).Msg("Publish")
			}

			zLog.Info().Stringer("event_id", eventID).Msg("published")

			cnt.Add(1)
			time.Sleep(publishDelayLong)
		}

		cnt.Wait()
		qm.Stop()
	})
}

func doPreTestDelay() {
	zLog.Warn().Msgf("waiting %v", preTestDelay)
	time.Sleep(preTestDelay)
}

func TestOOM(t *testing.T) {
	ctx := context.Background()
	eventIDs := make([]queuedproto.EventID, 0, 50)
	newEvents := sync.Mutex{}
	confLogLevel = logLevelDebug
	confFetchBatchSize = 1 // чтобы не выгребало пачками - а заняло все воркеры. не забываем, что это per-shard значение, и пачки будут по ShardNum штук

	qm := testManaged(t, func(qm *tp.StandaloneMaster, cnt *counter, doneChan <-chan struct{}) {
		for i := 0; i < 50; i++ {
			newEvents.Lock()
			eventID, err := qm.Publish(ctx, &eventSleeper{
				DurationMin: durationMin,
				DurationMax: durationMax,
				Percentage:  5, // чтобы события успешно завершались (удалялись) с низкой вероятностью, и их осталось достаточно для теста на кол-во воркеров
			})

			if err != nil {
				zLog.Panic().Err(err).Msg("Publish")
			}

			zLog.Info().Stringer("event_id", eventID).Msg("event published")

			eventIDs = append(eventIDs, eventID)

			cnt.Add(1)
			newEvents.Unlock()
			time.Sleep(publishDelay)
		}

		doPreTestDelay()

		zLog.Warn().Msg("dropping the soft limit")
		confOOMSoftLimit.Store("10Kb")

		time.Sleep(workerPoolSustainDelay)

		workersGot := cnt.Get()
		workersExpected := int32(float64(atomic.LoadInt32(&confWorkers)) * queueconfig.DefaultDownscaleRate)

		if workersGot > workersExpected {
			zLog.Panic().Msgf("too many workers when the soft memory limit is reached: got %d, expected: no more than %d", workersGot, workersExpected)
		}

		time.Sleep(preTestDelay)

		zLog.Warn().Msg("dropping the hard limit")
		confOOMHardLimit.Store("10Kb")

		select {
		case <-time.NewTimer(workerPoolSustainDelay).C:
			panic("must exit when the hard memory limit is reached")

		case <-doneChan:
		}
	})

	newEvents.Lock() // стопим цикл отправки новых сообщений
	zLog.Info().Interface("event_ids", eventIDs).Msg("deleting events")

	if err := qm.Queued().Delete(ctx, testSleeperQueueID, eventIDs); err != nil {
		zLog.Fatal().Err(err).Msg("Delete")
	}
}

func TestResize(t *testing.T) {
	ctx := context.Background()
	eventIDs := make([]queuedproto.EventID, 0, 50)
	newEvents := sync.Mutex{}
	confLogLevel = logLevelTrace
	confFetchBatchSize = 1 // чтобы не выгребало пачками - а заняло все воркеры

	qm := testManaged(t, func(qm *tp.StandaloneMaster, cnt *counter, doneChan <-chan struct{}) {
		newEvents.Lock()

		for i := 0; i < 50; i++ {
			eventID, err := qm.Publish(ctx, &eventSleeper{
				DurationMin: durationMin,
				DurationMax: durationMax,
				Percentage:  5,
			})

			if err != nil {
				zLog.Err(err).Msg("Publish")
			}

			zLog.Info().Stringer("event_id", eventID).Msg("event published")

			eventIDs = append(eventIDs, eventID)

			cnt.Add(1)
		}

		newEvents.Unlock()

		doPreTestDelay()

		setWorkersMax(workersMaxLimit, workerPoolSustainDelay)

		if workersGot := cnt.Get(); workersGot > workersMaxLimit {
			zLog.Panic().Msgf("got %d workers when workers_max=%d is set", workersGot, workersMaxLimit)
		}

		setWorkersMax(workersMaxDefault, preTestDelay)
		setWorkersMaxPerQueue(workersMaxLimit, workerPoolSustainDelay)

		if workersGot := cnt.Get(); workersGot > workersMaxLimit {
			zLog.Panic().Msgf("got %d workers when workers_max_per_queue=%d is set", workersGot, workersMaxLimit)
		}

		setWorkersMaxPerQueue(wokersMaxPerQueueDefault, time.Duration(0))
		setWorkersMax(0, workerPoolSustainDelay)

		if workersGot := cnt.Get(); workersGot > 0 {
			zLog.Panic().Msgf("got %d workers, expected 0", workersGot)
		}

		setWorkersMax(workersMaxDefault, preTestDelay)
		setWorkersMaxPerQueue(0, workerPoolSustainDelay)

		if workersGot := cnt.Get(); workersGot > 0 {
			zLog.Panic().Msgf("got %d workers, expected 0", workersGot)
		}

		qm.Stop()

		select {
		case <-time.NewTimer(workerPoolSustainDelay).C:
			panic("should exit earlier") // другого способа выйти из теста из новой горутины нет (а если мы до сюда добрались - значит qm.Stop завис)

		case <-doneChan:
		}
	})

	newEvents.Lock() // стопим цикл отправки новых сообщений
	zLog.Info().Interface("event_ids", eventIDs).Msg("deleting events")

	if err := qm.Queued().Delete(ctx, testSleeperQueueID, eventIDs); err != nil {
		zLog.Fatal().Err(err).Msg("Delete")
	}
}

// TestUpdateDelete - тест на race condition при удалении (RcItemLocked), он же - тест на ретраи (запускать через TestUpdateDeleteLoop, убрав Skip)
func TestUpdateDelete(t *testing.T) {
	ctx := context.Background()
	eventIDs := make([]queuedproto.EventID, 0, 50)
	newEvents := sync.Mutex{}
	confLogLevel = logLevelTrace
	confFetchBatchSize = 1 // чтобы не выгребало пачками - а заняло все воркеры

	qm := testManaged(t, func(qm *tp.StandaloneMaster, cnt *counter, doneChan <-chan struct{}) {
		newEvents.Lock()

		for i := 0; i < 50; i++ {
			eventID, err := qm.Publish(ctx, &eventSleeper{
				DurationMin: time.Duration(0),
				DurationMax: 20 * time.Millisecond,
				Percentage:  0,
			})

			if err != nil {
				zLog.Panic().Err(err).Msg("Publish")
			}

			zLog.Info().Stringer("event_id", eventID).Msg("event published")

			eventIDs = append(eventIDs, eventID)

			cnt.Add(1)
		}

		newEvents.Unlock()

		time.Sleep(publishDelayLong)
		qm.Stop()
	})

	newEvents.Lock() // стопим цикл отправки новых сообщений
	zLog.Info().Interface("event_ids", eventIDs).Msg("deleting events")

	if err := qm.Queued().Delete(ctx, testSleeperQueueID, eventIDs); err != nil {
		zLog.Fatal().Err(err).Msg("Delete")
	}
}

func TestUpdateDeleteLoop(t *testing.T) {
	t.Skip()

	for {
		TestUpdateDelete(t)
	}
}

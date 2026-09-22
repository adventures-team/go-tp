package tp

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"syscall"
	"time"

	advslog "github.com/adventures-team/go-adv-slog"

	"gitlab.adv.team/go/tp.git/workerpool"
)

// SignalReload - перечитывается конфиг (как и при изменении онлайнконфа)
const SignalReload = syscall.SIGHUP

// SignalsExit - graceful exit (дожидается завершения обработки всех событий, и выходит)
var SignalsExit = [...]os.Signal{syscall.SIGTERM, syscall.SIGINT}

type (
	queueIDWorkers struct {
		queueID QueueIDType
		workers int
	}

	// StandaloneMaster - управление пулом обработчиков очередей при помощи onlineconf.
	// Поддерживает все методы типа [Queue].
	//
	// Не путать с перловым менеджером очередей.
	StandaloneMaster struct {
		Queue
		pool          *workerpool.WorkerPool
		logger        *slog.Logger
		queuesRunning map[QueueIDType]bool
		oldScale      float64
	}
)

// NewStandaloneManager создаёт новый объект для управления пулом.
//
// Это - низкоуровневый интерфейс. Для типового кейса следует юзать обёртку [InitProcessor].
func NewStandaloneManager(queue Queue, pool *workerpool.WorkerPool, logger *slog.Logger) *StandaloneMaster {
	return &StandaloneMaster{
		Queue:         queue,
		pool:          pool,
		logger:        logger,
		queuesRunning: make(map[QueueIDType]bool),
		oldScale:      -1, // невозможное значение, чтобы при первом запуске конфиг применился полностью
	}
}

func (qm *StandaloneMaster) forceReload() {
	qm.oldScale = -1
	qm.Config().ClearErrCache()
}

// applyConfig применяет настройки из конфига к пулу очередей.
// Если параметр queue_ids не задан (либо был удалён/поломан с предыдущего вызова этого метода), не делает ничего (просто возвращает ошибку).
// Для остановки всех запущенных очередей надо задать пустой параметр.
// Если workers_max меньше кол-ва заданных очередей в queue_ids, то запускается по 1 обработчику на очередь, и выдаётся ошибка в лог (приоритета важности очереди у нас нет, параметр нужен для экстренного тушения аварий, поэтому овердизайнить тут не стоит).
// Параллельно вызывать applyConfig не надо (особенно с одним и тем же queuesRunning).
func (qm *StandaloneMaster) applyConfig(ctx context.Context) error {
	newLogLevel := qm.Config().LogLevel(ctx)
	if advslog.Level.Level() != newLogLevel {
		advslog.Level.Set(newLogLevel)
	}

	qm.logger.Info("reloading config")

	scale, err := qm.applyMemLimits(ctx)
	if err != nil {
		return err
	}

	if qm.oldScale == scale {
		return nil
	}

	confQueueIDs, err := qm.Config().QueueIDs()
	if err != nil {
		return err
	}

	workersTotal := 0
	workersMax, err := qm.Config().WorkersMaxCount()
	hasWorkersMax := err == nil
	queueWorkers := make([]queueIDWorkers, 0, len(confQueueIDs))

	for _, queueID := range confQueueIDs {
		if !qm.IsRegistered(queueID) {
			qm.logger.Error("skipping unregistered queue", advslog.Err(ErrUnknownQueue), "queue_id", queueID)
			continue
		}

		workers, err := qm.Config().WorkersCount(ctx, queueID)
		if err != nil {
			return err // параметр обязательный, не стартуем, если не указано глобально (т.к. если не указано для конкретно этой очереди - должно использоваться глобальное значение)
		}

		if workers <= 0 {
			continue
		}

		if workers = int(float64(workers) * scale); workers <= 0 {
			workers = 1
		}

		queueWorkers = append(queueWorkers, queueIDWorkers{
			queueID: queueID,
			workers: workers,
		})

		workersTotal += workers
	}

	qm.oldScale = scale

	if hasWorkersMax {
		if workersMax == 0 {
			if len(qm.queuesRunning) != 0 {
				qm.logger.Info("workers_max=0, stopping everything")
			} else {
				qm.logger.Info("workers_max=0, doing nothing")
			}

			for queueID := range qm.queuesRunning {
				qm.pool.SetWorkerCount(queueID, 0)
				qm.logger.Info("queue stopped", "queue_id", queueID)
				delete(qm.queuesRunning, queueID)
			}

			return nil
		}

		if workersMax < len(queueWorkers) {
			qm.logger.Error("please increase workers_max", "workers_max", workersMax, "queue_count", len(queueWorkers))
			workersMax = len(queueWorkers)
		}

		if excess := workersTotal - workersMax; excess > 0 {
			qm.logger.Warn("requested workers count exceed workers_max, sacrificing workers", "workers_max", workersMax, "workers_total", workersTotal, "sacrificed", excess)
			limitWorkers(queueWorkers, excess)
		}
	}

	qm.logger.Info("queue workers configured", "workers_total", workersTotal)
	qm.startStop(queueWorkers)

	return nil
}

func (qm *StandaloneMaster) applyMemLimits(ctx context.Context) (float64, error) {
	scale := float64(1)

	softLimit := qm.Config().OOMSoftLimit(ctx)
	hardLimit := qm.Config().OOMHardLimit(ctx)

	if softLimit != 0 && hardLimit != 0 {
		if softLimit > hardLimit {
			qm.logger.Warn("oom_soft_limit > oom_hard_limit, please check config", "oom_soft_limit", softLimit, "oom_hard_limit", hardLimit)
		}

		var memstats runtime.MemStats

		runtime.ReadMemStats(&memstats)

		if hardLimit != 0 && memstats.Sys > hardLimit {
			qm.logger.Error("hard memory limit reached", advslog.Err(ErrHardMemoryLimit), "mem_usage", memstats.Sys, "oom_hard_limit", hardLimit)
			return 0, ErrHardMemoryLimit
		}

		if softLimit != 0 && memstats.Sys > softLimit {
			scale = qm.Config().OOMSoftDownscaleRate(ctx)
			if qm.oldScale != scale {
				qm.logger.Warn("memory usage is over the soft limit, downscaling worker count", "mem_usage", memstats.Sys, "oom_soft_limit", softLimit, "scale", scale)
			}
		}
	}

	return scale, nil
}

func limitWorkers(queueWorkers []queueIDWorkers, excess int) {
	sort.Slice(queueWorkers, func(i, j int) bool {
		return queueWorkers[i].workers < queueWorkers[j].workers
	})

	// TODO придумать более быстрый алгоритм
	for excess > 0 {
		for i, qw := range queueWorkers {
			if qw.workers == 1 {
				break
			}

			queueWorkers[i].workers = qw.workers - 1

			if excess--; excess <= 0 {
				break
			}
		}
	}
}

func (qm *StandaloneMaster) startStop(queueWorkers []queueIDWorkers) {
	flipFlop := false // чтобы не заводить 2 мэпа - такой странный алгоритм TODO документировать его, либо переделать
	for _, old := range qm.queuesRunning {
		flipFlop = !old
		break
	}

	for _, qw := range queueWorkers {
		qm.pool.SetWorkerCount(qw.queueID, qw.workers)

		if _, ok := qm.queuesRunning[qw.queueID]; !ok {
			qm.logger.Info("queue started", "queue_id", qw.queueID, "worker_count", qw.workers)
		}

		qm.queuesRunning[qw.queueID] = flipFlop
	}

	for queueID, old := range qm.queuesRunning {
		if old != flipFlop {
			qm.pool.SetWorkerCount(queueID, 0)
			qm.logger.Info("queue stopped", "queue_id", queueID)
			delete(qm.queuesRunning, queueID)
		}
	}
}

// Run - для обработчиков очередей. Запуск управления пулом воркеров из конфига.
// Применяет конфиг сразу при вызове, а также при изменении конфига, и по сигналу SIGHUP.
// Если в конфиге при первом запуске обнаружены ошибки - пул воркеров останавливается, и функция возвращается.
// При изменении конфига/по сигналу выход/остановка происходят только при ошибке достижения oom_hard_limit.
// Для обычных ошибок - пишется ошибка в лог, и остаётся старый конфиг.
// Вызывать из main, при возврате - выходить.
//
// При возврате (после остановки пула) горутина менеджера конфига тоже останавливается,
// Run дожидается её выхода (иначе она продолжала бы жить и логгировать после Stop -
// в тестах это приводило к панике "Log in goroutine after test has completed").
func (qm *StandaloneMaster) Run() {
	ctx, cancel := context.WithCancel(advslog.NewContext(context.Background(), qm.logger))
	done := make(chan struct{})

	go func() { // менеджер конфига запускается асинхронно, т.к. просто посылает команды пулу
		defer close(done)

		qm.runGoro(ctx)
	}()

	qm.pool.Run() // пул должен запускаться синхронно, чтобы выход из Manage происходил после окончания всей обработки

	cancel()
	<-done
}

// runGoro крутит manage, перезапуская его после внутренних паник.
func (qm *StandaloneMaster) runGoro(ctx context.Context) {
	for {
		if qm.manage(ctx) {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(workerpool.InternalPanicDelay):
		}
	}
}

// manage возвращает true при штатном завершении (выход/останов пула/отмена контекста),
// false - после перехваченной паники (для перезапуска в runGoro).
func (qm *StandaloneMaster) manage(ctx context.Context) (exit bool) {
	defer func() {
		if reason := recover(); reason != nil {
			advslog.LogPanic(ctx, "panic in QueueManager.manage", reason)
		}
	}()

	err := qm.applyConfig(ctx)
	if err != nil {
		qm.pool.Stop() // Stop умеет работать асинхронно (т.е. Stop, вызванный до запуска пула, приведёт к его мгновенному выходу при запуске)
		qm.logger.Error("config error, exiting", advslog.Err(err))

		return true
	}

	reloadSigChan := make(chan os.Signal, 1)
	exitSigChan := make(chan os.Signal, 1)

	signal.Notify(reloadSigChan, SignalReload)
	defer signal.Stop(reloadSigChan)

	updateChan, err := qm.Config().Subscribe()
	if err != nil {
		advslog.FatalContext(ctx, "onlineconf.Subscribe", advslog.Err(err))
	}

	for _, sig := range SignalsExit {
		signal.Notify(exitSigChan, sig)
	}

	defer signal.Stop(exitSigChan)

	for {
		timer := time.NewTimer(qm.Config().OOMCheckPeriod(ctx))

		select {
		case <-ctx.Done():
			timer.Stop()
			return true

		case <-updateChan:
			qm.logger.Info("reloading config on onlineconf update")
			qm.forceReload()

		case sig := <-reloadSigChan:
			qm.logger.Info("reloading config on signal", "signal", sig.String())
			qm.forceReload()

		case sig := <-exitSigChan:
			qm.logger.Info("graceful exit on signal", "signal", sig.String())
			qm.pool.Stop()

			return true

		case <-timer.C:
		}

		timer.Stop()

		err := qm.applyConfig(ctx)
		if err != nil {
			if errors.Is(err, ErrHardMemoryLimit) {
				qm.pool.Stop()
				return true
			}

			qm.logger.Error("ignoring config changes on error", advslog.Err(err))
		}
	}
}

// Stop - враппер для WorkerPool.Stop. Останавливает обработку новых событий,
// по завершению запущенных обработчиков, метод Manage выходит.
func (qm *StandaloneMaster) Stop() {
	qm.pool.Stop()
}

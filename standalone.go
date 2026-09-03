package tp

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"gitlab.adv.team/go/tp.git/workerpool"
	"gitlab.adv.team/go/utils.git/mylog"
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
		logger        *zerolog.Logger
		queuesRunning map[QueueIDType]bool
		oldScale      float64
	}
)

// NewStandaloneManager создаёт новый объект для управления пулом.
//
// Это - низкоуровневый интерфейс. Для типового кейса следует юзать обёртку [InitProcessor].
func NewStandaloneManager(queue Queue, pool *workerpool.WorkerPool, logger *zerolog.Logger) *StandaloneMaster {
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
	if zerolog.GlobalLevel() != newLogLevel {
		zerolog.SetGlobalLevel(newLogLevel)
	}

	qm.logger.Info().Msg("reloading config")

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
			qm.logger.Err(ErrUnknownQueue).Uint16("queue_id", queueID).Send()
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
				qm.logger.Info().Msg("workers_max=0, stopping everything")
			} else {
				qm.logger.Info().Msg("workers_max=0, doing nothing")
			}

			for queueID := range qm.queuesRunning {
				qm.pool.SetWorkerCount(queueID, 0)
				qm.logger.Info().Uint16("queue_id", queueID).Msg("queue stopped")
				delete(qm.queuesRunning, queueID)
			}

			return nil
		}

		if workersMax < len(queueWorkers) {
			qm.logger.Error().Int("workers_max", workersMax).Int("queue_count", len(queueWorkers)).Msg("please increase workers_max")
			workersMax = len(queueWorkers)
		}

		if excess := workersTotal - workersMax; excess > 0 {
			qm.logger.Warn().Int("workers_max", workersMax).Int("workers_total", workersTotal).Msgf("requested workers count exceed workers_max, sacrificing %d workers", excess)
			limitWorkers(queueWorkers, excess)
		}
	}

	qm.logger.Info().Int("workers_total", workersTotal).Send()
	qm.startStop(queueWorkers)

	return nil
}

func (qm *StandaloneMaster) applyMemLimits(ctx context.Context) (float64, error) {
	scale := float64(1)

	softLimit := qm.Config().OOMSoftLimit(ctx)
	hardLimit := qm.Config().OOMHardLimit(ctx)

	if softLimit != 0 && hardLimit != 0 {
		if softLimit > hardLimit {
			qm.logger.Warn().Uint64("oom_soft_limit", softLimit).Uint64("oom_hard_limit", hardLimit).Msg("oom_soft_limit > oom_hard_limit, please check config")
		}

		var memstats runtime.MemStats

		runtime.ReadMemStats(&memstats)

		if hardLimit != 0 && memstats.Sys > hardLimit {
			qm.logger.Err(ErrHardMemoryLimit).Uint64("mem_usage", memstats.Sys).Uint64("oom_hard_limit", hardLimit).Send()
			return 0, ErrHardMemoryLimit
		}

		if softLimit != 0 && memstats.Sys > softLimit {
			scale = qm.Config().OOMSoftDownscaleRate(ctx)
			if qm.oldScale != scale {
				qm.logger.Warn().Uint64("mem_usage", memstats.Sys).Uint64("oom_soft_limit", softLimit).Float64("scale", scale).Msg("memory usage is over the soft limit, downscaling worker count")
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
			qm.logger.Info().Uint16("queue_id", qw.queueID).Int("worker_count", qw.workers).Msg("queue started")
		}

		qm.queuesRunning[qw.queueID] = flipFlop
	}

	for queueID, old := range qm.queuesRunning {
		if old != flipFlop {
			qm.pool.SetWorkerCount(queueID, 0)
			qm.logger.Info().Uint16("queue_id", queueID).Msg("queue stopped")
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
func (qm *StandaloneMaster) Run() {
	ctx := qm.logger.WithContext(context.Background())

	go qm.runGoro(ctx) // менеджер конфига запускается асинхронно, т.к. просто посылает команды пулу

	qm.pool.Run() // пул должен запускаться синхронно, чтобы выход из Manage происходил после окончания всей обработки
}

func (qm *StandaloneMaster) runGoro(ctx context.Context) {
	defer func() {
		if reason := recover(); reason != nil {
			mylog.LogPanic(qm.logger.Error(), reason).Msg("panic in QueueManager.manage")
			time.Sleep(workerpool.InternalPanicDelay)

			go qm.runGoro(ctx)
		}
	}()

	err := qm.applyConfig(ctx)
	if err != nil {
		qm.pool.Stop() // Stop умеет работать асинхронно (т.е. Stop, вызванный до запуска пула, приведёт к его мгновенному выходу при запуске)
		qm.logger.Err(err).Msg("config error, exiting")

		return
	}

	reloadSigChan := make(chan os.Signal, 1)
	exitSigChan := make(chan os.Signal, 1)

	signal.Notify(reloadSigChan, SignalReload)

	updateChan, err := qm.Config().Subscribe()
	if err != nil {
		qm.logger.Fatal().Err(err).Msg("onlineconf.Subscribe")
	}

	for _, sig := range SignalsExit {
		signal.Notify(exitSigChan, sig)
	}

	for {
		timer := time.NewTimer(qm.Config().OOMCheckPeriod(ctx))

		select {
		case <-updateChan:
			qm.logger.Info().Msg("reloading config on onlineconf update")
			qm.forceReload()

		case sig := <-reloadSigChan:
			qm.logger.Info().Msgf("reloading config on signal '%s'", sig)
			qm.forceReload()

		case sig := <-exitSigChan:
			qm.logger.Info().Msgf("graceful exit on signal '%s'", sig)
			qm.pool.Stop()

			return

		case <-timer.C:
		}

		timer.Stop()

		err := qm.applyConfig(ctx)
		if err != nil {
			if errors.Is(err, ErrHardMemoryLimit) {
				qm.pool.Stop()
				return
			}

			qm.logger.Err(err).Msg("ignoring config changes on error")
		}
	}
}

// Stop - враппер для WorkerPool.Stop. Останавливает обработку новых событий,
// по завершению запущенных обработчиков, метод Manage выходит.
func (qm *StandaloneMaster) Stop() {
	qm.pool.Stop()
}

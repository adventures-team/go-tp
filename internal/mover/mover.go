package mover

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	advslog "github.com/adventures-team/go-adv-slog"
)

// ErrInterrupted - режим one-shot прерван остановкой до опустошения всех очередей.
var ErrInterrupted = errors.New("interrupted before all queues were drained")

// attrQueueID - атрибут лога с номером очереди (как в tp).
const attrQueueID = "queue_id"

// Mover - супервизор переноса: держит набор очередей из
// queue_ids с их воркерами, перечитывает настройки, пишет строку прогресса и в режиме
// one-shot решает, когда очереди опустошены.
type Mover struct {
	conf     *settings
	src, dst Client
	logger   *slog.Logger

	reload chan struct{}   // [Mover.Reload]: перечитать queue_ids/workers/log_level
	exits  chan workerExit // воркеры сообщают о выходе

	// состояние ниже меняется только из горутины [Mover.Run]
	queues map[uint16]*queueState
	wg     sync.WaitGroup // все горутины воркеров
	lastID int            // номер последнего запущенного воркера (для логов)
}

// queueState - состояние очереди в супервизоре.
type queueState struct {
	counters  *queueCounters
	active    map[int]context.CancelFunc // работающие (не отменённые) воркеры по номерам
	running   int                        // ещё не вышедшие воркеры, включая отменённые
	emptySeen bool                       // one-shot: какой-то воркер увидел очередь пустой
	done      bool                       // one-shot: очередь опустошена (перепроверено супервизором)
	last      counterSnapshot            // счётчики на момент предыдущей строки прогресса
}

// workerExit - сообщение воркера о выходе из цикла.
type workerExit struct {
	queueID uint16
	id      int
	empty   bool // one-shot: очередь пуста
}

// New создаёт супервизор: conf - поддерево onlineconf с настройками утилиты, src и dst -
// клиенты кластеров A (источник) и B (приёмник).
func New(conf Conf, src, dst Client, logger *slog.Logger) *Mover {
	return &Mover{
		conf:   newSettings(conf),
		src:    src,
		dst:    dst,
		logger: logger,
		reload: make(chan struct{}, 1),
		exits:  make(chan workerExit),
		queues: make(map[uint16]*queueState),
	}
}

// Reload просит супервизор перечитать queue_ids, workers и log_level (например, по SIGHUP).
// Остальные параметры читаются при каждом использовании и в перечитывании не нуждаются.
func (m *Mover) Reload() {
	select {
	case m.reload <- struct{}{}:
	default:
	}
}

// Run выполняет перенос до отмены ctx (режим сервиса) или до опустошения всех очередей
// (режим one-shot, nil). Отмена ctx в режиме one-shot до опустошения очередей - [ErrInterrupted];
// ошибка в обязательных параметрах при старте возвращается как есть. Остановка дожидается
// завершения текущих итераций воркеров.
func (m *Mover) Run(ctx context.Context) error {
	ctx = advslog.NewContext(ctx, m.logger)

	if err := m.conf.validate(); err != nil {
		return err
	}

	updates := m.subscribe()

	m.apply(ctx)

	if m.conf.oneShot() && len(m.queues) == 0 {
		m.logger.Warn("queue_ids is empty, nothing to move")

		return nil
	}

	timer := time.NewTimer(m.conf.statPeriod(ctx))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.Info("stopping")

			return m.finish()

		case <-updates:
			m.logger.Info("reloading config on onlineconf update")
			m.conf.clearErrCache()
			m.apply(ctx)

		case <-m.reload:
			m.conf.clearErrCache()
			m.apply(ctx)

		case <-timer.C:
			m.apply(ctx)
			m.logProgress(ctx)
			timer.Reset(m.conf.statPeriod(ctx))

		case exit := <-m.exits:
			if m.onWorkerExit(ctx, exit) {
				m.logger.Info("all queues are drained")

				return m.finish()
			}
		}
	}
}

// subscribe подписывается на изменения поддерева настроек. Без подписки (ошибка) настройки
// перечитываются по SIGHUP и раз в stat_period; nil-канал в select никогда не срабатывает.
func (m *Mover) subscribe() <-chan struct{} {
	updates, err := m.conf.conf.SubscribeSubtree("/")
	if err != nil {
		m.logger.Warn("onlineconf subscription failed, the queue list is reread periodically only", advslog.Err(err))

		return nil
	}

	return updates
}

// apply применяет queue_ids, workers и log_level: останавливает очереди, исчезнувшие из списка,
// запускает новые, подгоняет число воркеров. Ошибка в queue_ids - лог, прежний набор очередей
// продолжает работать.
func (m *Mover) apply(ctx context.Context) {
	if level := m.conf.logLevel(ctx); advslog.Level.Level() != level {
		advslog.Level.Set(level)
	}

	ids, err := m.conf.queueIDs()
	if err != nil {
		m.logger.Error("ignoring the queue list on error", advslog.Err(err))

		return
	}

	workers := m.conf.workers(ctx)
	oneShot := m.conf.oneShot()

	for id, qs := range m.queues {
		if !slices.Contains(ids, id) {
			m.stopQueue(id, qs)
		}
	}

	for _, id := range ids {
		qs, ok := m.queues[id]
		if !ok {
			qs = &queueState{counters: &queueCounters{}, active: make(map[int]context.CancelFunc)}
			m.queues[id] = qs

			m.logger.Info("queue started", attrQueueID, id, "workers", workers)
		}

		if qs.done && oneShot {
			continue
		}

		qs.done = false // one-shot выключили - опустошённые очереди снова обслуживаются
		m.resize(ctx, id, qs, workers)
	}
}

// stopQueue останавливает воркеры очереди и забывает её.
func (m *Mover) stopQueue(id uint16, qs *queueState) {
	for wid, cancel := range qs.active {
		cancel()
		delete(qs.active, wid)
	}

	delete(m.queues, id)

	m.logger.Info("queue stopped", append([]any{attrQueueID, id}, qs.counters.attrs()...)...)
}

// resize подгоняет число работающих воркеров очереди под wanted.
func (m *Mover) resize(ctx context.Context, id uint16, qs *queueState, wanted int) {
	for wid, cancel := range qs.active {
		if len(qs.active) <= wanted {
			break
		}

		cancel()
		delete(qs.active, wid)
	}

	for len(qs.active) < wanted {
		m.startWorker(ctx, id, qs)
	}
}

// startWorker запускает горутину воркера очереди.
func (m *Mover) startWorker(ctx context.Context, id uint16, qs *queueState) {
	m.lastID++
	wid := m.lastID

	wctx, cancel := context.WithCancel(ctx)
	qs.active[wid] = cancel
	qs.running++

	w := &queueWorker{
		conf:     m.conf,
		src:      m.src,
		dst:      m.dst,
		queueID:  id,
		logger:   m.logger.With(attrQueueID, id, "worker", wid),
		counters: qs.counters,
	}

	m.wg.Add(1)

	go func() {
		defer m.wg.Done()

		empty := w.run(wctx)

		cancel()

		m.exits <- workerExit{queueID: id, id: wid, empty: empty}
	}()
}

// onWorkerExit учитывает выход воркера. Когда все воркеры очереди вышли и хотя бы один видел
// её пустой, супервизор перепроверяет статистику (сосед мог отложить события уже после
// проверки воркера): пусто - очередь опустошена, иначе воркеры перезапускаются после poll_delay.
// Возвращает true, когда опустошены все очереди.
func (m *Mover) onWorkerExit(ctx context.Context, exit workerExit) (allDone bool) {
	qs, ok := m.queues[exit.queueID]
	if !ok {
		return false // очередь уже убрана из списка
	}

	delete(qs.active, exit.id)
	qs.running--

	if exit.empty {
		qs.emptySeen = true
	}

	if qs.running > 0 || !qs.emptySeen {
		return false
	}

	qs.emptySeen = false
	logger := m.logger.With(attrQueueID, exit.queueID)

	items, _, locked, err := queueStatSum(ctx, m.src, exit.queueID)

	switch {
	case err != nil:
		logger.Error("error getting queue stat, retrying", advslog.Err(err))
	case items != 0:
		logger.Info("events remain in the source, waiting", "items", items, "locked", locked)
	default:
		qs.done = true
		logger.Info("queue drained", qs.counters.attrs()...)

		return m.allDone()
	}

	time.AfterFunc(m.conf.pollDelay(ctx), m.Reload)

	return false
}

// allDone сообщает, опустошены ли все очереди.
func (m *Mover) allDone() bool {
	for _, qs := range m.queues {
		if !qs.done {
			return false
		}
	}

	return len(m.queues) > 0
}

// finish дожидается выхода всех воркеров (их контексты уже отменены) и возвращает итог:
// [ErrInterrupted], если в режиме one-shot очереди не опустошены.
func (m *Mover) finish() error {
	for _, qs := range m.queues {
		for wid, cancel := range qs.active {
			cancel()
			delete(qs.active, wid)
		}
	}

	done := make(chan struct{})

	go func() {
		m.wg.Wait()
		close(done)
	}()

	for {
		select {
		case <-m.exits: // воркеры отчитываются о выходе и после остановки
		case <-done:
			m.logSummary()

			if m.conf.oneShot() && !m.allDone() {
				return ErrInterrupted
			}

			return nil
		}
	}
}

// logProgress пишет строку прогресса по каждой очереди: счётчики (суммарно и за период)
// и статистика очереди в A.
func (m *Mover) logProgress(ctx context.Context) {
	for _, id := range m.queueIDs() {
		qs := m.queues[id]
		current := qs.counters.snapshot()
		attrs := []any{attrQueueID, id, "workers", len(qs.active)}
		attrs = append(attrs, qs.counters.attrs()...)
		attrs = append(attrs, slog.Group("period", current.delta(qs.last).attrs()...))
		qs.last = current

		items, active, locked, err := queueStatSum(ctx, m.src, id)
		if err != nil {
			attrs = append(attrs, advslog.Err(err))
		} else {
			attrs = append(attrs, "src_items", items, "src_active", active, "src_locked", locked)
		}

		m.logger.Info("progress", attrs...)
	}
}

// logSummary пишет итоговую сводку по очередям.
func (m *Mover) logSummary() {
	for _, id := range m.queueIDs() {
		qs := m.queues[id]

		m.logger.Info("summary", append([]any{attrQueueID, id, "drained", qs.done}, qs.counters.attrs()...)...)
	}
}

// queueIDs возвращает отсортированный список очередей супервизора.
func (m *Mover) queueIDs() []uint16 {
	ids := make([]uint16, 0, len(m.queues))

	for id := range m.queues {
		ids = append(ids, id)
	}

	slices.Sort(ids)

	return ids
}

package tp

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/rs/zerolog"

	advmetricsset "github.com/my-mail-ru/go-adv-metrics/set"
	"github.com/my-mail-ru/go-queuedproto"

	"gitlab.cebc.tools/go/tp.git/queuedclient"
	"gitlab.cebc.tools/go/tp.git/workerpool"
)

type (
	queuePoller struct {
		*queueImpl
		logger    *zerolog.Logger
		isStopped int32
	}

	// Stat is used by managed master. Not an application metrics.
	Stat struct {
		processed atomic.Uint32
		errors    atomic.Uint32
	}

	queueWorker struct {
		*queueImpl
		queueID    QueueIDType
		events     *eventList
		ctx        context.Context
		isStopped  *int32
		stat       *Stat
		inProgress *metrics.Gauge
		started    time.Time
	}
)

var (
	_ workerpool.Poller  = &queuePoller{}
	_ workerpool.Stopper = &queuePoller{}
	_ workerpool.Worker  = &queueWorker{}
)

func (s *Stat) Processed() uint32 {
	return s.processed.Load()
}

func (s *Stat) Errors() uint32 {
	return s.errors.Load()
}

// NewPoller возвращает объект для опроса новых событий из WorkerPool
func (qi *queueImpl) NewPoller(logger *zerolog.Logger) workerpool.Poller {
	return &queuePoller{
		queueImpl: qi,
		logger:    logger,
	}
}

// EntityIDName - для WorkerPool. Возвращает "queue_id"
func (*queuePoller) EntityIDName() string {
	return "queue_id"
}

// Poll - получение событий из указанной очереди из WorkerPool
func (qp *queuePoller) Poll(queueID QueueIDType) workerpool.Worker {
	logger := qp.logger.With().Uint16("queue_id", queueID).Logger()

	if atomic.LoadInt32(&qp.isStopped) != 0 {
		logger.Debug().Msg("queuePoller.Poll skipped during shutdown")
		return nil
	}

	ctx := withBaseLog(context.Background(), &logger)

	// TODO handle panics

	// logger.Debug().Msg("queue poll")

	events, err := qp.getActiveEvents(ctx, queueID, 0)
	if err != nil {
		var errMulti queuedclient.ErrMulti

		if errors.As(err, &errMulti) {
			for _, errSingle := range errMulti {
				if !errors.Is(errSingle, queuedproto.ErrUnknownQueueType) { // отсутствие очереди в queued - это не ошибка, просто ещё не было событий
					logger.Err(errSingle).Msg("error getting active events")
				}
			}
		} else {
			logger.Err(err).Msg("error getting active events")
		}

		if events == nil { // если есть ошибка, но события тоже есть - работаем дальше
			return nil
		}
	}

	if len(events.baseEvents) == 0 {
		logger.Trace().Msg("no events received")
		return nil
	}

	ret := &queueWorker{
		queueImpl: qp.queueImpl,
		queueID:   queueID,
		events:    events,
		ctx:       ctx,
		isStopped: &qp.isStopped,
		stat:      &Stat{},
	}

	if (qp.metrics != advmetricsset.Set{}) {
		qp.metrics.Name("tp_events_total").Int("queue_id", int(queueID)).GetOrCreateCounter().Add(len(events.baseEvents))

		ret.started = time.Now()
		ret.inProgress = qp.metrics.Name("tp_events_in_progress").Int("queue_id", int(queueID)).GetOrCreateGauge(nil)

		ret.inProgress.Add(float64(len(events.baseEvents)))
	}

	return ret
}

func (qp *queuePoller) Stop() {
	atomic.StoreInt32(&qp.isStopped, 1)
}

func (qw *queueWorker) Work() {
	logger := logFromCtx(qw.ctx)

	defer func() { // TODO даже без обработки паник, хотя бы страхуемся обновлением всех обработанных событий и деферим необработанные
		if err := qw.events.commit(qw.ctx, qw.stat); err != nil {
			logger.Err(err).Msg("failed to apply events processing status")
		}

		if qw.inProgress != nil {
			qw.inProgress.Add(float64(-len(qw.events.baseEvents)))
			qw.MetricsSet().Name("tp_event_duration_seconds").Int("queue_id", int(qw.queueID)).GetOrCreateHistogram().UpdateDuration(qw.started)
		}
	}()

	handlersByVersion, ok := qw.handlerMap[qw.queueID]
	if !ok {
		logger.Error().Uint16("queue_id", qw.queueID).Msg("queue_id isn't registered")
		return
	}

	if logger.GetLevel() <= zerolog.DebugLevel {
		eventIDs := make([]string, len(qw.events.baseEvents))
		for i, ev := range qw.events.baseEvents {
			eventIDs[i] = ev.EventID.String()
		}

		logger.Debug().Int("events_count", len(qw.events.baseEvents)).Strs("event_ids", eventIDs).Msg("queue poll status")
	}

	for version, events := range qw.events.eventsByVersion {
		if atomic.LoadInt32(qw.isStopped) != 0 {
			logger.Debug().Msg("stopping queueWorker.Work() loop")
			break
		}

		if events == nil { // TODO remove if an array is replaced with a map
			continue
		}

		hInfo, ok := handlersByVersion[uint8(version)]
		if !ok {
			logger.Error().Int("version", version).Msg("version isn't registered")
			continue
		}

		logger.Debug().Interface("events", events).Msg("new events received")

		err := hInfo.handler(qw.ctx, events)
		if err != nil {
			// TODO cancel?
			logger.Err(err).Send() // Msg тут не нужен - чтобы из логов нагрепывали реальную причину, а не эту строку
		}
	}
}

func (qw *queueWorker) Stat() workerpool.WorkerStat {
	return qw.stat
}

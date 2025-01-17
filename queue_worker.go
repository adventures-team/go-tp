package tp

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/rs/zerolog"

	"gitlab.corp.my.games/adventures/go/queuedproto.git"
	"gitlab.corp.my.games/adventures/go/tp.git/queuedclient"
	"gitlab.corp.my.games/adventures/go/tp.git/workerpool"
)

type (
	queuePoller struct {
		*Queue
		logger    *zerolog.Logger
		isStopped int32
	}

	Stat struct {
		processed atomic.Uint32
		errors    atomic.Uint32
	}

	queueWorker struct {
		*Queue
		queueID   QueueIDType
		events    *EventList
		ctx       context.Context
		isStopped *int32
		stat      *Stat
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
func (q *Queue) NewPoller(logger *zerolog.Logger) workerpool.Poller {
	return &queuePoller{
		Queue:  q,
		logger: logger,
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

	events, err := qp.GetActiveEvents(ctx, queueID, 0)
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

	return &queueWorker{
		Queue:     qp.Queue,
		queueID:   queueID,
		events:    events,
		ctx:       ctx,
		isStopped: &qp.isStopped,
		stat:      &Stat{},
	}
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

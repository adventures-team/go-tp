package tp

import (
	"context"
	"errors"

	"github.com/rs/zerolog"

	"gitlab.corp.mail.ru/mygo/queuedproto.git"
	"gitlab.corp.mail.ru/mygo/tp.git/workerpool"
)

type (
	queuePoller struct {
		*Queue
		logger *zerolog.Logger
	}

	queueWorker struct {
		*Queue
		events *eventList
		ctx    context.Context
	}
)

var (
	_ workerpool.Poller = queuePoller{}
	_ workerpool.Worker = queueWorker{}
)

// NewPoller возвращает объект для опроса новых событий из WorkerPool
func (q *Queue) NewPoller(logger *zerolog.Logger) workerpool.Poller {
	return queuePoller{
		Queue:  q,
		logger: logger,
	}
}

// EntityIDName - для WorkerPool. Возвращает "queue_id"
func (*Queue) EntityIDName() string {
	return "queue_id"
}

// Poll - получение событий из указанной очереди из WorkerPool
func (qp queuePoller) Poll(queueID QueueIDType) workerpool.Worker {
	logger := qp.logger.With().Uint16("queue_id", queueID).Logger()
	ctx := withBaseLog(context.Background(), &logger)

	// TODO handle panics

	// logger.Debug().Msg("queue poll")

	events, err := qp.GetActiveEvents(ctx, queueID, 0)
	if err != nil {
		if errors.Is(err, queuedproto.ErrUnknownQueueType) {
			return nil // отсутствие очереди в queued - это не ошибка, просто ещё не было событий
		}

		logger.Err(err).Msg("error getting active events")

		return nil
	}

	if len(events.events) == 0 {
		logger.Trace().Msg("no events received")
		return nil
	}

	return queueWorker{
		Queue:  qp.Queue,
		events: events,
		ctx:    ctx,
	}
}

func (qw queueWorker) Work() {
	logger := logFromCtx(qw.ctx)

	defer func() { // TODO даже без обработки паник, хотя бы страхуемся обновлением всех обработанных событий и деферим необработанные
		if err := qw.events.commit(qw.ctx); err != nil {
			logger.Err(err).Msg("failed to apply events processing status")
		}
	}()

	if logger.GetLevel() <= zerolog.DebugLevel {
		eventIDs := make([]uint64, len(qw.events.events))
		for i, handler := range qw.events.events {
			eventIDs[i] = handler.GetEvent().id
		}

		logger.Debug().Int("events_count", len(qw.events.events)).Uints64("event_ids", eventIDs).Msg("queue poll status")
	}

	for _, handler := range qw.events.events {
		event := handler.GetEvent() // TODO подумать, вытащить ли апи событий в интерфейс Handler. учитывая, что обработчик и так всё это видит (т.к. является методом своей структуры), наверно, оверхед (компилера/человека, возможно, рантайма) превысит пользу.
		if event.flags&evIsProcessed != 0 {
			continue
		}

		evLog := logger.With().Uint64("event_id", event.id).Logger()
		evCtx := evLog.WithContext(qw.ctx) // TODO cancel? timeouts?
		// TODO handle panics, cancel context

		evLog.Debug().Interface("event", handler).Msg("new event received")

		err := handler.Handle(evCtx)
		if err != nil {
			evLog.Err(err).Send() // Msg тут не нужен - чтобы из логов нагрепывали реальную причину, а не эту строку
			// TODO cancel?
			continue
		}
	}
}

package tp

import (
	"context"
	"errors"

	"github.com/rs/zerolog"

	"gitlab.corp.mail.ru/mygo/queuedproto.git"
	"gitlab.corp.mail.ru/mygo/tp.git/workerpool"
)

type queueWorker struct {
	*Queue
	events *eventList
	zLog   *zerolog.Logger
}

var (
	_ workerpool.Poller = &Queue{}
	_ workerpool.Worker = queueWorker{}
)

// EntityIDName - для WorkerPool. Возвращает "queue_id"
func (*Queue) EntityIDName() string {
	return "queue_id"
}

// Poll - получение событий из указанной очереди из WorkerPool
func (queue *Queue) Poll(queueID QueueIDType) workerpool.Worker {
	zLog := queue.logger.With().Uint16("queue_id", queueID).Logger()

	// TODO handle panics

	//zLog.Debug().Msg("queue poll")

	events, err := queue.GetActiveEvents(queueID, 0)
	if err != nil {
		if errors.Is(err, queuedproto.ErrUnknownQueueType) {
			return nil // отсутствие очереди в queued - это не ошибка, просто ещё не было событий
		}

		zLog.Err(err).Msg("error getting active events")

		return nil
	}

	if len(events.events) == 0 {
		zLog.Trace().Msg("no events received")
		return nil
	}

	return queueWorker{
		Queue:  queue,
		events: events,
		zLog:   &zLog,
	}
}

func (qw queueWorker) Work() {
	ctx := context.Background() // TODO зачем тут? мы же не будем грохать всю обработку в случае паники вне цикла?

	defer func() { // TODO даже без обработки паник, хотя бы страхуемся обновлением всех обработанных событий и деферим необработанные
		if err := qw.events.commit(); err != nil {
			qw.zLog.Err(err).Msg("failed to apply events processing status")
		}
	}()

	if qw.zLog.GetLevel() <= zerolog.DebugLevel {
		eventIDs := make([]uint64, len(qw.events.events))
		for i, handler := range qw.events.events {
			eventIDs[i] = handler.GetEvent().id
		}

		qw.zLog.Debug().Int("events_count", len(qw.events.events)).Uints64("event_ids", eventIDs).Msg("queue poll status")
	}

	for _, handler := range qw.events.events {
		event := handler.GetEvent() // TODO подумать, вытащить ли апи событий в интерфейс Handler. учитывая, что обработчик и так всё это видит (т.к. является методом своей структуры), наверно, оверхед (компилера/человека, возможно, рантайма) превысит пользу.
		if event.flags&evIsProcessed != 0 {
			continue
		}

		evLog := qw.zLog.With().Uint64("event_id", event.id).Logger()
		evCtx := evLog.WithContext(ctx) // TODO cancel? timeouts?
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

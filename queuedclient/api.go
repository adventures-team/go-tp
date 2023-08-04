package queuedclient

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"gitlab.corp.mail.ru/e-mail-ru/easygo/iproto"
	"gitlab.corp.mail.ru/mygo/queuedproto.git"
)

// GetActive получает активные события из очереди queueID изо всех шардов одновременно.
//
// Если count не задано (<=0), используется значение из конфига (fetch_batch_size).
// count задаётся per shard (т.е. может приехать count*qc.ShardNum() записей).
//
// В случае, если на некоторых шардах произошла ошибка, но с других данные успешно вернулись - возвращаются данные, ошибка не возвращается,
// и о случившемся пишется в лог. Если ошибка произошла везде/данных нет - возвращается ошибка.
//
// Ретраи в случае ошибок не производятся.
func (qc *QueuedClient) GetActive(ctx context.Context, queueID uint16, count int) (queuedproto.ItemList, error) {
	if count <= 0 {
		count = qc.Config.FetchBatchSize(ctx, queueID)
	}

	result, err := qc.getBroadcast(ctx, queuedproto.ReqGetActive{
		StorageType: queueID,
		Count:       uint32(count),
	}, count)

	if err != nil {
		err = fmt.Errorf("GetActive: queue_id=%d, %w", queueID, err)
	}

	return result, err
}

// GetByIDsBroadcast запрашивает события из очереди queueID с указанными ID изо всех шардов одновременно.
//
// # ID событий указываются без номера шарда, обычными uint64
//
// Ретраи в случае ошибок не производятся.
func (qc *QueuedClient) GetByIDsBroadcast(ctx context.Context, queueID uint16, eventIDs []uint64) (queuedproto.ItemList, error) {
	result, err := qc.getBroadcast(ctx, queuedproto.ReqGetItems{
		StorageType: queueID,
		IDs:         eventIDs,
	}, len(eventIDs))

	if err != nil {
		err = fmt.Errorf("GetByIDs: queue_id=%d, event_ids=%v: %w", queueID, eventIDs, err)
	}

	return result, err
}

func (qc *QueuedClient) getBroadcast(ctx context.Context, req queuedproto.Request, count int) (queuedproto.ItemList, error) {
	resultFunc, getResult := qc.collectItemList(count)
	err := qc.MakeBroadcastRequest(ctx, req, resultFunc)

	return getResult(), err
}

// TODO нечитаемо )
// порефакторить на объект с методами ResultFunc и Get, ResultFunc передавать как bound method функциям, посылающим запросы
func (qc *QueuedClient) collectItemList(estimated int) (ResultFunc, func() queuedproto.ItemList) {
	resultChan := make(chan queuedproto.ItemList)
	doneChan := make(chan struct{})
	multiResult := queuedproto.ItemList{Items: make([]queuedproto.QueueItem, 0, estimated*int(qc.ShardNum()))}

	go func() {
		for shardResult := range resultChan {
			multiResult.Items = append(multiResult.Items, shardResult.Items...)
		}

		close(doneChan)
	}()

	return func(shard uint, data []byte) error {
			var shardResult queuedproto.ItemList

			if _, err := iproto.Unpack(data, &shardResult); err != nil {
				return err
			}

			for i := range shardResult.Items {
				shardResult.Items[i].Shard = shard
			}

			resultChan <- shardResult

			return nil
		},
		func() queuedproto.ItemList {
			close(resultChan)
			<-doneChan

			return multiResult
		}
}

// GetByIDs запрашивает события из очереди queueID с указанными ID из указанных шардов параллельно.
//
// Ретраи в случае ошибок не производятся.
func (qc *QueuedClient) GetByIDs(ctx context.Context, queueID uint16, eventIDs []queuedproto.EventID) (queuedproto.ItemList, error) {
	resultFunc, getResult := qc.collectItemList(len(eventIDs))

	err := qc.MakeMulticastRequest(
		ctx,
		qc.Config.FetchBatchSize(ctx, queueID),
		eventIDs,
		func(ids []uint64) queuedproto.Request {
			return queuedproto.ReqGetItems{
				StorageType: queueID,
				IDs:         ids,
			}
		},
		resultFunc,
	)

	return getResult(), err
}

// Delete удаляет указанные события параллельно изо всех необходимых шардов пачками по update_batch_size событий.
func (qc *QueuedClient) Delete(ctx context.Context, queueID uint16, eventIDs []queuedproto.EventID) error {
	err := qc.MakeMulticastRequest(
		ctx,
		qc.Config.UpdateBatchSize(ctx, queueID),
		eventIDs,
		func(ids []uint64) queuedproto.Request {
			return queuedproto.ReqDeleteItems{
				StorageType: queueID,
				IDs:         ids,
			}
		},
		nil,
	)

	if err != nil {
		return fmt.Errorf("Delete: queue_id=%d: %w", queueID, err)
	}

	return nil
}

// Defer устанавливает задержку для указанных событий. Если задержка не указана (delay == 0), используется значение defer_time из конфига.
//
// События выполняются в порядке увеличения времени срабатывания, поэтому можно использовать отрицательную задержку в качестве
// приоритета (чем больше по модулю отрицательное значение задержки, тем выше приоритет).
//
// Обновления производятся пачками по update_batch_size событий.
func (qc *QueuedClient) Defer(ctx context.Context, queueID uint16, delay time.Duration, eventIDs []queuedproto.EventID) error {
	if delay == 0 {
		delay = qc.Config.DeferTime(ctx, queueID)
	}

	timestampType := queuedproto.TimestampRelativePlus

	if delay < 0 {
		timestampType = queuedproto.TimestampRelativeMinus
		delay = -delay
	}

	seconds := uint32(delay.Round(time.Second) / time.Second)

	err := qc.MakeMulticastRequest(
		ctx,
		qc.Config.UpdateBatchSize(ctx, queueID),
		eventIDs,
		func(ids []uint64) queuedproto.Request {
			return queuedproto.ReqUpdateItems{
				StorageType:   queueID,
				UnixTime:      seconds,
				IDs:           ids,
				TimestampType: timestampType,
			}
		},
		nil,
	)

	if err != nil {
		return fmt.Errorf("Defer: queue_id=%d: %w", queueID, err)
	}

	return nil
}

// Publish отправляет событие в очередь. Если eventID.ID == 0, queued создаёт новый ID, который возвращается.
// Если задан eventID.Shard == BalanceShard, шард выбирается по round robin.
// Если шард задан, а ID - нет, происходит автоинкремент ID в указанном шарде.
//
// Если в конфиге задано initial_delay, событие сработает с задержкой.
//
// В случае получения не-логической ошибки (!queuedproto.IsSoftError(err)), производится ещё /queueds/multi/retry_count попыток
// с задержкой /queueds/multi/retry_delay. Если после всех этих попыток ошибка повторяется - она возвращается.
//
// Если задан eventID.Shard == BalanceShard, то каждый ретрай посылается в следующий шард.
//
// ID запроса (queuedproto.ReqAddItem.ReqID) для всех ретраев отправляется один и тот же, на случай,
// если у нас шардов меньше, чем retry_count (например, 1), и 1й запрос ушёл, но ответ потерялся.
//
// Ошибки обработки результата (iproto.Unpack) не приводят к ретраям, т.к., скорее всего, являются ошибками прикладного уровня.
func (qc *QueuedClient) Publish(ctx context.Context, queueID uint16, eventID queuedproto.EventID, flags uint32, eventData []byte) (queuedproto.EventID, error) {
	initialDelay := qc.Config.InitialDelay(ctx, queueID)

	req := queuedproto.ReqAddItem{
		Pid:         qc.pid,
		ReqID:       atomic.AddUint32(&reqID, 1),
		StorageType: queueID,
		ID:          eventID.ID,
		UnixTime:    uint32(time.Now().Add(initialDelay).Unix()),
		Data:        eventData,
		Flags:       flags,
	}

	shard, respBytes, err := qc.MakeUnicastRequest(ctx, eventID.Shard, req)
	eventID.Shard = shard

	if err != nil {
		if flags&queuedproto.FlagIgnoreWrongItem != 0 && errors.Is(err, queuedproto.ErrWrongItem) && eventID.ID != 0 { // вообще непонятно, как эта ошибка может возникнуть при eventID == 0 - поэтому, если такое случится, пусть падает
			return eventID, nil
		}

		return queuedproto.EventID{}, fmt.Errorf("Publish: queue_id=%d: %w", queueID, err)
	}

	if _, err = iproto.Unpack(respBytes, &eventID.ID); err != nil {
		return queuedproto.EventID{}, fmt.Errorf("Publish: queue_id=%d: Unpack: %w", queueID, err)
	}

	return eventID, nil
}

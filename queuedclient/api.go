package queuedclient

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

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
func (qc *QueuedClient) GetActive(ctx context.Context, queueID uint16, count int) (queuedproto.ItemList, error) {
	if count <= 0 {
		count = qc.FetchBatchSize(queueID)
	}

	result, err := qc.getBroadcast(ctx, queuedproto.ReqGetActive{
		StorageType: queueID,
		Count:       uint32(count),
	}, count)

	if err != nil {
		return queuedproto.ItemList{}, fmt.Errorf("GetActive: queue_id=%d, %w", queueID, err)
	}

	return result, nil
}

// GetByIDsBroadcast запрашивает события из очереди queueID с указанными ID изо всех шардов одновременно.
//
// ID событий указываются без номера шарда, обычными uint64
func (qc *QueuedClient) GetByIDsBroadcast(ctx context.Context, queueID uint16, eventIDs []uint64) (queuedproto.ItemList, error) {
	result, err := qc.getBroadcast(ctx, queuedproto.ReqGetItems{
		StorageType: queueID,
		IDs:         eventIDs,
	}, len(eventIDs))

	if err != nil {
		return queuedproto.ItemList{}, fmt.Errorf("GetByIDs: queue_id=%d, event_ids=%v: %w", queueID, eventIDs, err)
	}

	return result, nil
}

func (qc *QueuedClient) getBroadcast(ctx context.Context, req queuedproto.Request, count int) (queuedproto.ItemList, error) {
	result := queuedproto.ItemList{Items: make([]queuedproto.QueueItem, 0, count*int(qc.ShardNum()))}
	err := qc.MakeBroadcastRequest(ctx, req, itemListResultFunc(&result))

	return result, err
}

func itemListResultFunc(result *queuedproto.ItemList) ResultFunc {
	return func(shard uint, data []byte) error {
		var shardResult queuedproto.ItemList

		if _, err := iproto.Unpack(data, &shardResult); err != nil {
			return err
		}

		for i := range shardResult.Items {
			shardResult.Items[i].Shard = shard
		}

		result.Items = append(result.Items, shardResult.Items...)

		return nil
	}
}

// GetByIDs запрашивает события из очереди queueID с указанными ID из указанных шардов параллельно.
func (qc *QueuedClient) GetByIDs(ctx context.Context, queueID uint16, eventIDs []queuedproto.EventID) (queuedproto.ItemList, error) {
	result := queuedproto.ItemList{Items: make([]queuedproto.QueueItem, 0, len(eventIDs)*int(qc.ShardNum()))}

	err := qc.MakeMulticastRequest(
		ctx,
		qc.FetchBatchSize(queueID),
		eventIDs,
		func(ids []uint64) queuedproto.Request {
			return queuedproto.ReqGetItems{
				StorageType: queueID,
				IDs:         ids,
			}
		},
		itemListResultFunc(&result),
	)

	return result, err
}

func (qc *QueuedClient) Delete(ctx context.Context, queueID uint16, eventIDs []queuedproto.EventID) error {
	err := qc.MakeMulticastRequest(
		ctx,
		qc.UpdateBatchSize(queueID),
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
// Изменяется не более update_batch_size событий за раз.
func (qc *QueuedClient) Defer(ctx context.Context, queueID uint16, delay time.Duration, eventIDs []queuedproto.EventID) error {
	if delay == 0 {
		var err error
		delay, err = qc.DeferTime(queueID)

		if err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Msg("config error (default is used)")
		}
	}

	when := uint32(time.Now().Add(delay).Unix())

	err := qc.MakeMulticastRequest(
		ctx,
		qc.UpdateBatchSize(queueID),
		eventIDs,
		func(ids []uint64) queuedproto.Request {
			return queuedproto.ReqUpdateItems{
				StorageType:   queueID,
				UnixTime:      when,
				IDs:           ids,
				TimestampType: queuedproto.TimestampAbsolute,
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
// Если задан eventID.Shard == BalanceShard, шард выбирается по round robin (т.о. есть возможность автогенерации id и в явно заданном шарде).
//
// Если в конфиге задано initial_delay, событие сработает с задержкой.
func (qc *QueuedClient) Publish(ctx context.Context, queueID uint16, eventID queuedproto.EventID, flags uint32, eventData []byte) (queuedproto.EventID, error) {
	initialDelay, err := qc.InitialDelay(queueID)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("config error (default is used)")
	}

	req := queuedproto.ReqAddItem{
		Pid:         qc.pid,
		ReqID:       atomic.AddUint32(&reqID, 1),
		StorageType: queueID,
		ID:          eventID.ID,
		UnixTime:    uint32(time.Now().Add(initialDelay).Unix()),
		Data:        eventData,
		Flags:       flags,
	}

	shard := qc.balanceShard(eventID.Shard)
	respBytes, err := qc.MakeUnicastRequest(ctx, shard, req)

	if err != nil && !(flags&queuedproto.FlagIgnoreWrongItem != 0 && errors.Is(err, queuedproto.ErrWrongItem) && eventID.ID != 0) { // вообще непонятно, как эта ошибка может возникнуть при eventID == 0 - поэтому, если такое случится, пусть падает
		return queuedproto.EventID{}, fmt.Errorf("Publish: queue_id=%d: %w", queueID, err)
	}

	if _, err = iproto.Unpack(respBytes, &eventID.ID); err != nil {
		return queuedproto.EventID{}, fmt.Errorf("Publish: queue_id=%d: error reading response: %w", queueID, err)
	}

	eventID.Shard = shard

	return eventID, nil
}

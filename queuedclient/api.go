package queuedclient

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/my-mail-ru/go-queuedproto"
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
// ID событий указываются без номера шарда, обычными uint64
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

			if _, err := shardResult.UnmarshalIProto(data); err != nil {
				return err
			}

			for i := range shardResult.Items {
				shardResult.Items[i].EventID.Shard = shard
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
// TODO может быть, игнорить [queuedproto.ErrItemLocked] и просто писать его в лог?
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

// Defer устанавливает задержку обработки для указанных событий. Если задержка не указана (delay == 0), используется значение defer_time из конфига.
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

// SetActivationTime устанавливает время активации для указанных событий.
// Обновления производятся пачками по update_batch_size событий.
func (qc *QueuedClient) SetActivationTime(ctx context.Context, queueID uint16, atime time.Time, eventIDs []queuedproto.EventID) error {
	err := qc.MakeMulticastRequest(
		ctx,
		qc.Config.UpdateBatchSize(ctx, queueID),
		eventIDs,
		func(ids []uint64) queuedproto.Request {
			return queuedproto.ReqUpdateItems{
				StorageType:   queueID,
				UnixTime:      uint32(atime.Unix()),
				IDs:           ids,
				TimestampType: queuedproto.TimestampAbsolute,
			}
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("SetActivationTime: queue_id=%d: %w", queueID, err)
	}

	return nil
}

// PublishOption - опции [QueuedClient.Publish]: [PublishActivationTime] и [PublishInitialDelay].
type PublishOption func(*PublishOptions)

// PublishActivationTime устанавливает время обработки нового события. Если не установлено - см. [PublishInitialDelay]
func PublishActivationTime(t time.Time) PublishOption {
	return func(opt *PublishOptions) {
		opt.activationTime = t
		opt.needDelay = true
	}
}

// PublishInitialDelay устанавливает задержку обработки нового события. Если не установлено - используется заданное в конфиге значение.
func PublishInitialDelay(d time.Duration) PublishOption {
	return func(opt *PublishOptions) {
		opt.initialDelay = d
		opt.needDelay = true
	}
}

type PublishOptions struct {
	activationTime time.Time
	initialDelay   time.Duration
	needDelay      bool
}

// Publish отправляет событие в очередь. Если eventID.ID == 0, queued создаёт новый ID, который возвращается.
// Если задан eventID.Shard == [BalanceShard], шард выбирается по round robin.
// Если шард задан, а ID - нет, происходит автоинкремент ID в указанном шарде.
//
// Можно задать время активации события при помощи опции [PublishActivationTime] или задержку [PublishInitialDelay].
// Если эти опции не заданы, используется задержка из конфига initial_delay.
// Задержка, заданная при помощи PublishInitialDelay(0) считается заданной явно, дефолтная задержка из конфига в этом случае не берётся.
// Если это значение не задано для очереди или глобально - событие активируется без задержки.
//
// В случае получения не-логической ошибки (!queuedproto.IsSoftError(err)), производится ещё /queueds/multi/retry_count попыток
// с задержкой /queueds/multi/retry_delay. Если после всех этих попыток ошибка повторяется - она возвращается.
//
// Если задан eventID.Shard == [BalanceShard], то каждый ретрай посылается в следующий шард.
//
// ID запроса (queuedproto.ReqAddItem.ReqID) для всех ретраев отправляется один и тот же, на случай,
// если у нас шардов меньше, чем retry_count (например, 1), и 1й запрос ушёл, но ответ потерялся.
//
// Ошибки обработки результата (iproto.Unmarshal) не приводят к ретраям, т.к., скорее всего, являются ошибками прикладного уровня.
func (qc *QueuedClient) Publish(ctx context.Context, queueID uint16, eventID queuedproto.EventID, flags uint32, eventData []byte, opts ...PublishOption) (queuedproto.EventID, error) {
	var opt PublishOptions

	for _, f := range opts {
		f(&opt)
	}

	if opt.activationTime.IsZero() {
		if !opt.needDelay {
			opt.initialDelay = qc.Config.InitialDelay(ctx, queueID)
		}

		opt.activationTime = time.Now().Add(opt.initialDelay)
	}

	req := queuedproto.ReqAddItem{
		Pid:         qc.pid,
		ReqID:       atomic.AddUint32(&reqID, 1),
		StorageType: queueID,
		ID:          eventID.ID,
		UnixTime:    uint32(opt.activationTime.Unix()),
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

	if _, err = eventID.UnmarshalIProto(respBytes); err != nil {
		return queuedproto.EventID{}, fmt.Errorf("Publish: queue_id=%d: Unpack: %w", queueID, err)
	}

	return eventID, nil
}

// Replace заменяет существующее событие в очереди. ID события должно быть обязательно задано.
// Если время срабатывания не установлено через опции, оно не изменяется (при помощи [queuedproto.SkipTimeUpdate])
//
// В queued есть старый баг (2010 или раньше), из-за которого время активации при использовании запроса обновления записи не меняется.
// В перловой реализации аддонов есть костыли на этот счёт.
// Данная реализация учитывает этот баг, время обновляется отдельным запросом, поэтому опциями
// [PublishActivationTime] или [PublishInitialDelay] пользоваться можно.
//
// Возвращается признак установки времени активации, и ошибка.
func (qc *QueuedClient) Replace(ctx context.Context, queueID uint16, eventID queuedproto.EventID, eventData []byte, opts ...PublishOption) (bool, error) {
	if eventID.ID == 0 {
		return false, errors.New("QueuedClient.Replace: missing event ID")
	}

	req := queuedproto.ReqFullUpdate{
		StorageType: queueID,
		UnixTime:    queuedproto.SkipTimeUpdate,
		Items: []queuedproto.UpdQueueItem{{
			EventID: eventID,
			Data:    eventData,
		}},
	}

	var opt PublishOptions

	for _, f := range opts {
		f(&opt)
	}

	if opt.needDelay {
		if !opt.activationTime.IsZero() {
			req.UnixTime = uint32(opt.activationTime.Unix())
		} else {
			req.UnixTime = uint32(time.Now().Add(opt.initialDelay).Unix())
		}
	}

	_, _, err := qc.MakeUnicastRequest(ctx, eventID.Shard, req)
	if err != nil {
		return false, fmt.Errorf("Replace: queue_id=%d: %w", queueID, err)
	}

	if opt.needDelay {
		// ReqFullUpdate doesn't really set activation time.
		// this is a VERY (pre-2010) old hack
		// perl/queue/lib/MR/Queue/Extensions.pm:140
		if !opt.activationTime.IsZero() {
			err = qc.SetActivationTime(ctx, queueID, opt.activationTime, []queuedproto.EventID{eventID})
		} else {
			if opt.initialDelay == 0 { // XXX a dirty hack to use a zero delay (since protocol doesn't handle subsecond resolution) instead of using the default
				opt.initialDelay = time.Microsecond
			}

			err = qc.Defer(ctx, queueID, opt.initialDelay, []queuedproto.EventID{eventID})
		}
	}

	return opt.needDelay, err
}

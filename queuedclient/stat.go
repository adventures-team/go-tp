package queuedclient

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/adventures-team/go-queuedproto"
)

// QueueStatVersion - версия запроса статистики очереди (поле Version в [queuedproto.ReqQueueStat]).
// Это CURRENT_GET_QUEUE_STAT_VERSION из исходников queued:
//
//	static const UCHAR8 CURRENT_GET_QUEUE_STAT_VERSION = 1;
//
// TODO перенести в go-queuedproto рядом с ReqQueueStat при следующем обновлении модуля.
const QueueStatVersion = 1

// QueueStat запрашивает статистику очереди queueID ([queuedproto.ReqQueueStat]) изо всех шардов
// одновременно. Результат - по шардам (индекс - номер шарда, нумерация с 0). Шард, в котором
// такой очереди нет ([queuedproto.ErrUnknownQueueType]), даёт нулевую статистику.
//
// В отличие от [QueuedClient.GetActive], частичный результат не возвращается: если статистику
// не удалось получить хотя бы с одного шарда, возвращается ошибка - для проверки "очередь пуста"
// неполные данные бесполезны.
//
// Ретраи в случае ошибок не производятся.
func (qc *QueuedClient) QueueStat(ctx context.Context, queueID uint16) ([]queuedproto.RespQueueStat, error) {
	var mu sync.Mutex

	stats := make([]queuedproto.RespQueueStat, qc.ShardNum())

	req := queuedproto.ReqQueueStat{
		Version:     QueueStatVersion,
		StorageType: queueID,
	}

	err := qc.MakeBroadcastRequest(ctx, req, func(shard uint, data []byte) error {
		var stat queuedproto.RespQueueStat

		if _, err := stat.UnmarshalIProto(data); err != nil {
			return err
		}

		mu.Lock()
		stats[shard] = stat
		mu.Unlock()

		return nil
	})

	if err = dropUnknownQueueErrors(err); err != nil {
		return nil, fmt.Errorf("QueueStat: queue_id=%d: %w", queueID, err)
	}

	return stats, nil
}

// dropUnknownQueueErrors убирает из мультишардовой ошибки ответы "unknown queue type": для
// запросов, где отсутствие очереди в шарде - штатный случай (нулевая статистика).
// Возвращает nil, если других ошибок не осталось.
func dropUnknownQueueErrors(err error) error {
	var errs ErrMulti

	if !errors.As(err, &errs) {
		return err
	}

	rest := make(ErrMulti, 0, len(errs))

	for _, shardErr := range errs {
		if !errors.Is(shardErr, queuedproto.ErrUnknownQueueType) {
			rest.Append(shardErr)
		}
	}

	return rest.ToError()
}

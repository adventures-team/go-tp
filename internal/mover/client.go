package mover

import (
	"context"
	"time"

	"github.com/adventures-team/go-queuedproto"

	"github.com/adventures-team/go-tp/queuedclient"
)

// Client - подмножество методов [queuedclient.QueuedClient], которым пользуется утилита.
// Интерфейс - ради моков в тестах (test/mock/mover).
type Client interface {
	// ShardNum возвращает количество шардов кластера.
	ShardNum() uint

	// GetActive получает активные события очереди изо всех шардов (count - на шард).
	GetActive(ctx context.Context, queueID uint16, count int) (queuedproto.ItemList, error)

	// GetByIDs запрашивает события с указанными ID из указанных в них шардов.
	GetByIDs(ctx context.Context, queueID uint16, eventIDs []queuedproto.EventID) (queuedproto.ItemList, error)

	// Publish отправляет событие в очередь (ID 0 - назначает queued, [queuedclient.BalanceShard] - шард по round robin).
	Publish(ctx context.Context, queueID uint16, eventID queuedproto.EventID, flags uint32, eventData []byte,
		opts ...queuedclient.PublishOption) (queuedproto.EventID, error)

	// Delete удаляет события из очереди.
	Delete(ctx context.Context, queueID uint16, eventIDs []queuedproto.EventID) error

	// Defer откладывает события на указанное время.
	Defer(ctx context.Context, queueID uint16, delay time.Duration, eventIDs []queuedproto.EventID) error

	// QueueStat запрашивает статистику очереди по шардам.
	QueueStat(ctx context.Context, queueID uint16) ([]queuedproto.RespQueueStat, error)
}

var _ Client = (*queuedclient.QueuedClient)(nil)

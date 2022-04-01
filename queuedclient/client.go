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
	"gitlab.corp.mail.ru/mygo/tp.git/queueconfig"
)

const (
	confPathServer  = "/queueds/main"
	confPathTimeout = "/queueds/main/timeout"

	defaultTimeout      = 2 // seconds TODO onlineconf.GetDuration
	defaultPingInterval = 5 * time.Second
)

var reqID uint32 // per process! не per-QueuedClient. в случае рандомных id, если последовательность будет сквозная у всех "pid", хуже от этого не станет, а вот если внутри процесса с разными объектаи будет совпадение reqID - хуже станет

// ErrEmptyResponse - queued ответил пустым пакетом
var ErrEmptyResponse = errors.New("empty response from queued")

// QueuedClient - низкоуровневый клиент queued
type QueuedClient struct {
	*queueconfig.Config
	endpoint *iproto.Pool
	logger   *zerolog.Logger
	pid      uint32
}

// New инициализирует клиент, и коннектится к серверам, указанным в конфиге. Не вызывать, если не нужны сетевые коннекты
func New(conf *queueconfig.Config, logger *zerolog.Logger) (*QueuedClient, error) {
	pid, err := getPID(conf.OnlineConf)
	if err != nil {
		return nil, err
	}

	servers := conf.GetString(confPathServer, "")
	timeout := time.Duration(conf.GetInt(confPathTimeout, defaultTimeout)) * time.Second

	hasServers := false
	endpoint := (*iproto.Pool)(nil)

	for _, server := range queueconfig.CSVRegex.Split(servers, -1) {
		if server == "" {
			continue
		}

		endpoint, err = iproto.Dial(context.Background(), "tcp", server, &iproto.PoolConfig{ // TODO прогрепать вообще все его опции по исходнику iproto
			ConnectTimeout: timeout,
			DialTimeout:    timeout,
			RedialForever:  true,
			// TODO defaultPingInterval
		})

		_ = defaultPingInterval

		if err != nil {
			return nil, fmt.Errorf("Dial %s: %w", server, err)
		}

		hasServers = true
	}

	if !hasServers {
		return nil, errors.New("no servers defined in " + confPathServer)
	}

	return &QueuedClient{
		Config:   conf,
		endpoint: endpoint,
		logger:   logger,
		pid:      pid,
	}, nil
}

func (qc *QueuedClient) makeReqRaw(req queuedproto.Request) ([]byte, error) {
	ctx := context.Background() // TODO XXX

	reqBytes, err := iproto.Pack(req)
	if err != nil {
		return nil, err
	}

	return qc.endpoint.Call(ctx, req.Cmd(), reqBytes)
}

func (qc *QueuedClient) makeReq(req queuedproto.Request, respPtr interface{}) error {
	respBytes, err := qc.makeReqRaw(req)
	if err != nil {
		return err
	}

	if len(respBytes) == 0 {
		return ErrEmptyResponse
	}

	if err = queuedproto.ErrorByRetCode(respBytes[0]); err != nil {
		return err
	}

	if respPtr == nil {
		return nil
	}

	_, err = iproto.Unpack(respBytes[1:], respPtr)

	return err
}

// GetActive получает активные события из очереди queueID. Если count не задано (<=0), используется значение из конфига (fetch_batch_size)
func (qc *QueuedClient) GetActive(queueID uint16, count int) (queuedproto.ItemList, error) {
	if count <= 0 {
		count = qc.FetchBatchSize(queueID)
	}

	respBytes, err := qc.makeReqRaw(queuedproto.ReqGetActive{
		StorageType: queueID,
		Count:       uint32(count),
	})

	if err != nil {
		return queuedproto.ItemList{}, fmt.Errorf("GetActive: queue_id=%d: %w", queueID, err)
	}

	if respBytes[0] == queuedproto.RcNoActiveItems {
		return queuedproto.ItemList{}, nil
	}

	if err := queuedproto.ErrorByRetCode(respBytes[0]); err != nil {
		return queuedproto.ItemList{}, fmt.Errorf("GetActive: queue_id=%d: %w", queueID, err)
	}

	result := queuedproto.ItemList{}

	if _, err := iproto.Unpack(respBytes[1:], &result); err != nil {
		return queuedproto.ItemList{}, fmt.Errorf("GetActive: queue_id=%d: %w", queueID, err)
	}

	return result, nil
}

func (qc *QueuedClient) GetByIDs(queueID uint16, eventIDs []uint64) (queuedproto.ItemList, error) {
	result := queuedproto.ItemList{}

	err := qc.makeReq(queuedproto.ReqGetItems{
		StorageType: queueID,
		IDs:         eventIDs,
	}, &result)

	if err != nil {
		return queuedproto.ItemList{}, fmt.Errorf("GetByIDs: queue_id=%d, event_ids=%v: %w", queueID, eventIDs, err)
	}

	return result, nil
}

func (qc *QueuedClient) Delete(queueID uint16, eventIDs []uint64) error {
	batchSize := qc.UpdateBatchSize(queueID)

	for start := 0; start < len(eventIDs); start += batchSize {
		end := start + batchSize
		if end > len(eventIDs) {
			end = len(eventIDs)
		}

		batch := eventIDs[start:end]

		err := qc.makeReq(queuedproto.ReqDeleteItems{
			StorageType: queueID,
			IDs:         batch,
		}, nil)

		if err != nil {
			return fmt.Errorf("Delete: queue_id=%d, event_ids=%v: %w", queueID, batch, err)
		}
	}

	return nil
}

// Defer устанавливает задержку для указанных событий. Если задержка не указана (delay == 0), используется значение defer_time из конфига
func (qc *QueuedClient) Defer(queueID uint16, delay time.Duration, eventIDs []uint64) error {
	if delay == 0 {
		var err error
		delay, err = qc.DeferTime(queueID)

		if err != nil {
			qc.logger.Warn().Err(err).Msg("config error (default is used)")
		}
	}

	batchSize := qc.UpdateBatchSize(queueID)

	for start := 0; start < len(eventIDs); start += batchSize {
		end := start + batchSize
		if end > len(eventIDs) {
			end = len(eventIDs)
		}

		batch := eventIDs[start:end]

		err := qc.makeReq(queuedproto.ReqUpdateItems{
			StorageType:   queueID,
			UnixTime:      uint32(time.Now().Add(delay).Unix()),
			IDs:           batch,
			TimestampType: queuedproto.TimestampAbsolute,
		}, nil)

		if err != nil {
			return fmt.Errorf("Defer: queue_id=%d, event_ids=%v: %w", queueID, batch, err)
		}
	}

	return nil
}

// Publish отправляет событие в очередь. Если eventID == 0, queued создаёт новый ID, который возвращается.
// Если в конфиге задано initial_delay, событие сработает с задержкой.
func (qc *QueuedClient) Publish(queueID uint16, eventID uint64, flags uint32, eventData []byte) (uint64, error) {
	initialDelay, err := qc.InitialDelay(queueID)
	if err != nil {
		qc.logger.Warn().Err(err).Msg("config error (default is used)")
	}

	req := queuedproto.ReqAddItem{
		Pid:         qc.pid,
		ReqID:       atomic.AddUint32(&reqID, 1),
		StorageType: queueID,
		ID:          eventID,
		UnixTime:    uint32(time.Now().Add(initialDelay).Unix()),
		Data:        eventData,
		Flags:       flags,
	}

	err = qc.makeReq(req, &eventID)

	if err != nil {
		if flags&queuedproto.FlagIgnoreWrongItem != 0 && errors.Is(err, queuedproto.ErrWrongItem) && eventID != 0 { // вообще непонятно, как эта ошибка может возникнуть при eventID == 0 - поэтому, если такое случится, пусть падает
			return eventID, nil
		}

		return 0, fmt.Errorf("Publish: queue_id=%d: %w", queueID, err)
	}

	return eventID, nil
}

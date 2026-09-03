package queuedclient

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/rs/zerolog"

	advmetricsset "github.com/my-mail-ru/go-adv-metrics/set"
	"github.com/my-mail-ru/go-queuedproto"

	easygo "gitlab.adv.team/go/easygo.git/iproto"
	"gitlab.adv.team/go/tp.git/internal"
	"gitlab.adv.team/go/utils.git/mylog"
)

type (
	// RequestFunc вызывается функцией [QueuedClient.MakeMulticastRequest] для формирования запроса для указанных ID (относящихся к одному шарду)
	RequestFunc func(eventIDs []uint64) queuedproto.Request

	// ResultFunc вызывается функциями [QueuedClient.MakeBroadcastRequest] и [QueuedClient.MakeMulticastRequest] для сохранения результата, полученного из указанного шарда.
	// Результат надо сохранять либо через замыкание - либо передавать в качестве этой функции bound method объекта, куда сохраняются данные.
	//
	// Эта функция вызывается из разных горутин и должна быть thread-safe (не использовать прямой append/обычный map без лока! использовать каналы, [sync.Map] и т.д.)
	ResultFunc func(shard uint, result []byte) error

	withQueueID interface {
		QueueID() uint16
	}
)

func (qc *QueuedClient) balanceShard(shard uint) uint {
	if shard != BalanceShard {
		return shard
	}

	return uint(atomic.AddUint32(&qc.roundRobin, 1)) % qc.ShardNum()
}

// MakeUnicastRequest посылает запрос req к указанному шарду.
//
// В случае получения не-логической ошибки (!queuedproto.IsSoftError(err)), производится ещё /queueds/multi/retry_count попыток
// с задержкой /queueds/multi/retry_delay. Если после всех этих попыток ошибка повторяется - она возвращается.
//
// Если задан shard == [BalanceShard], шард выбирается по round robin. В этом случае, каждый ретрай посылается в следующий шард.
func (qc *QueuedClient) MakeUnicastRequest(ctx context.Context, shard uint, req queuedproto.Request) (usedShard uint, respBytes []byte, err error) {
	if shard != BalanceShard && shard >= qc.ShardNum() {
		return 0, nil, fmt.Errorf("%w %d", ErrInvalidShard, shard)
	}

	reqBytes, err := req.MarshalIProto(internal.AllocBytes())
	if err != nil {
		return 0, nil, err
	}

	cmd := req.Cmd()
	retry := 0
	queueID := 0

	if qID, ok := req.(withQueueID); ok {
		queueID = int(qID.QueueID())
	}

	for {
		usedShard = qc.balanceShard(shard)
		endpoint := qc.shards[usedShard]

		var (
			reqStarted time.Time
			inProgress *metrics.Gauge
		)

		if (qc.metrics != advmetricsset.Set{}) {
			reqStarted = time.Now()
			inProgress = qc.metrics.Name("tp_queued_in_progress").Str("queued", endpoint.RemoteAddr().String()).Str("cmd", cmd.String()).Int("queue_id", queueID).GetOrCreateGauge(nil)

			qc.metrics.Name("tp_queued_total").Str("queued", endpoint.RemoteAddr().String()).Str("cmd", cmd.String()).Int("queue_id", queueID).GetOrCreateCounter().Inc()
			inProgress.Inc()
		}

		respBytes, err = endpoint.Call(ctx, uint32(cmd), reqBytes)

		if inProgress != nil {
			inProgress.Dec()
			qc.metrics.Name("tp_queued_duration_seconds").Str("queued", endpoint.RemoteAddr().String()).Str("cmd", cmd.String()).Int("queue_id", queueID).GetOrCreateHistogram().UpdateDuration(reqStarted)
		}

		if err == nil {
			if len(respBytes) == 0 {
				return usedShard, nil, ErrEmptyResponse
			}

			if err = queuedproto.ErrorByRetCode(respBytes[0]); err == nil || queuedproto.IsSoftError(err) {
				return usedShard, respBytes[1:], err
			}

			if inProgress != nil {
				// err is low-cardinality
				qc.metrics.Name("tp_queued_errors").Str("queued", endpoint.RemoteAddr().String()).Str("cmd", cmd.String()).Int("queue_id", queueID).Str("err", err.Error()).GetOrCreateCounter().Inc()
			}
		} else if inProgress != nil {
			qc.metrics.Name("tp_queued_errors").Str("queued", endpoint.RemoteAddr().String()).Str("cmd", cmd.String()).Int("queue_id", queueID).Str("err", "network").GetOrCreateCounter().Inc()
		}

		retry++

		retryCount := qc.Config.GetInt(ConfRetryCount, DefaultRetryCount)

		zlEv := zerolog.Ctx(ctx).Err(err).
			Uint("shard", usedShard).
			Stringer("queued", endpoint.RemoteAddr()).
			Int("retry", retry).
			Int("retry_count", retryCount).
			Stringer("cmd", cmd).
			Interface("req", req)

		if retry >= retryCount {
			zlEv.Msg("retry count exceeded")
			return usedShard, nil, err
		}

		retryDelay := qc.Config.GetDuration(ctx, ConfRetryDelay, DefaultRetryDelay)

		zlEv.Dur("retry_delay", retryDelay).Msg("retry on error")
		time.Sleep(retryDelay)
	}
}

// MakeBroadcastRequest посылает запрос req параллельно во все шарды. После получения ответа от каждого шарда,
// вызывается resFunc.
//
// Возврат ошибки не говорит о том, что запрос не выполнен нигде - кол-во успешно
// выполненных запросов (и полученных данных) следует осуществлять в resFunc.
//
// resFunc вызывается из разных горутин и должна быть thread-safe.
//
// Ретраи в случае ошибок не производятся, т.к. функция предназначена для get-запросов, текущая логика
// которых (опрос событий) не ломается от разовых сбоев некоторых шардов.
//
// TODO раскопипастить broad/multicast запросы - когда полностью определимся с их логикой.
func (qc *QueuedClient) MakeBroadcastRequest(ctx context.Context, req queuedproto.Request, resFunc ResultFunc) error {
	reqBytes, err := req.MarshalIProto(internal.AllocBytes())
	if err != nil {
		return err
	}

	cmd := req.Cmd()
	queueID := 0
	errChan := make(chan error)
	errs := make(ErrMulti, 0, len(qc.shards))

	if qID, ok := req.(withQueueID); ok {
		queueID = int(qID.QueueID())
	}

	for shard, endpoint := range qc.shards {
		go func(shard uint, endpoint *easygo.Pool) {
			var (
				err           error
				errMetricSent bool
				reqStarted    time.Time
				inProgress    *metrics.Gauge
			)

			defer func() {
				if reason := recover(); reason != nil {
					mylog.LogPanic(zerolog.Ctx(ctx).Error(), reason).Uint("shard", shard).Stringer("queued", endpoint.RemoteAddr()).Msg("panic in MakeBroadcastRequest")

					if inProgress != nil {
						qc.metrics.Name("tp_queued_errors").Str("queued", endpoint.RemoteAddr().String()).Str("cmd", cmd.String()).Int("queue_id", queueID).Str("err", "panic").GetOrCreateCounter().Inc()
					}

					err = ErrPanic
					errMetricSent = true
				}

				if err == nil {
					errChan <- nil
				} else {
					if !errMetricSent && inProgress != nil {
						qc.metrics.Name("tp_queued_errors").Str("queued", endpoint.RemoteAddr().String()).Str("cmd", cmd.String()).Int("queue_id", queueID).Str("err", "network").GetOrCreateCounter().Inc()
					}

					errChan <- fmt.Errorf("shard #%d (%s): %w", shard, endpoint.RemoteAddr().String(), err)
				}
			}()

			zerolog.Ctx(ctx).Trace().Uint("shard", shard).Stringer("cmd", cmd).Interface("req", req).Msg("iproto.Call")

			if (qc.metrics != advmetricsset.Set{}) {
				reqStarted = time.Now()
				inProgress = qc.metrics.Name("tp_queued_in_progress").Str("queued", endpoint.RemoteAddr().String()).Str("cmd", cmd.String()).Int("queue_id", queueID).GetOrCreateGauge(nil)

				qc.metrics.Name("tp_queued_total").Str("queued", endpoint.RemoteAddr().String()).Str("cmd", cmd.String()).Int("queue_id", queueID).GetOrCreateCounter().Inc()
				inProgress.Inc()
			}

			respBytes, err := endpoint.Call(ctx, uint32(cmd), reqBytes)

			if inProgress != nil {
				inProgress.Dec()
				qc.metrics.Name("tp_queued_duration_seconds").Str("queued", endpoint.RemoteAddr().String()).Str("cmd", cmd.String()).Int("queue_id", queueID).GetOrCreateHistogram().UpdateDuration(reqStarted)
			}

			if err != nil {
				return
			}

			zerolog.Ctx(ctx).Trace().Uint("shard", shard).Stringer("cmd", cmd).Bytes("resp", respBytes).Msg("iproto.Call")

			if len(respBytes) == 0 {
				err = ErrEmptyResponse
				return
			}

			if err = queuedproto.ErrorByRetCode(respBytes[0]); err != nil {
				if inProgress != nil {
					// err is low-cardinality
					qc.metrics.Name("tp_queued_errors").Str("queued", endpoint.RemoteAddr().String()).Str("cmd", cmd.String()).Int("queue_id", queueID).Str("err", err.Error()).GetOrCreateCounter().Inc()

					errMetricSent = true
				}

				return
			}

			if resFunc == nil || len(respBytes) == 1 {
				return
			}

			err = resFunc(shard, respBytes[1:])
		}(uint(shard), endpoint)
	}

	for range qc.shards {
		if err := <-errChan; err != nil {
			errs.Append(err)
		}
	}

	close(errChan)

	return errs.ToError()
}

// MakeMulticastRequest посылает запросы для указанных eventIDs в нужные шарды. Для формирования запроса по списку
// ID вызывается reqFunc. После получения ответа от каждого шарда, вызывается resFunc.
//
// resFunc вызывается из разных горутин и должна быть thread-safe.
//
// В случае получения не-логической ошибки (!queuedproto.IsSoftError(err)), производится ещё /queueds/multi/retry_count попыток
// с задержкой /queueds/multi/retry_delay. Если после всех этих попыток ошибка повторяется - она вносится в список ошибок шардов.
//
// Ошибки обработки результата в resFunc не приводят к ретраям, т.к., скорее всего, являются ошибками прикладного уровня.
//
// Возврат ошибки не говорит о том, что запрос не выполнен нигде - кол-во успешно
// выполненных запросов (и полученных данных) следует осуществлять в resFunc.
func (qc *QueuedClient) MakeMulticastRequest(ctx context.Context, batchSize int, eventIDs []queuedproto.EventID, reqFunc RequestFunc, resFunc ResultFunc) error {
	shardMap := make(map[uint][]uint64, qc.ShardNum())

	for _, evID := range eventIDs {
		if evID.Shard >= qc.ShardNum() {
			return fmt.Errorf("%d %w", evID.Shard, ErrInvalidShard)
		}

		shardIDs, ok := shardMap[evID.Shard]
		if !ok {
			shardIDs = make([]uint64, 0, len(eventIDs)) // TODO см. аналогичный коммент в box/parallel.go
		}

		shardMap[evID.Shard] = append(shardIDs, evID.ID)
	}

	errChan := make(chan error)
	errs := make(ErrMulti, 0, len(qc.shards))
	wg := &sync.WaitGroup{} // ошибок может быть больше шардов, если len(ids) < batchSize, поэтому кроме канала нужно wg.
	// по этому же, nil-ошибки можно не посылать

	wg.Add(len(shardMap))

	for shard, ids := range shardMap {
		go qc.reqMultiProcessShard(ctx, batchSize, shard, ids, errChan, wg, reqFunc, resFunc)
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	for err := range errChan {
		errs.Append(err)
	}

	return errs.ToError()
}

func (qc *QueuedClient) reqMultiProcessShard(ctx context.Context, batchSize int, shard uint, ids []uint64, errChan chan<- error, wg *sync.WaitGroup, reqFunc RequestFunc, resFunc ResultFunc) {
	endpoint := qc.shards[shard]

	defer func() {
		if reason := recover(); reason != nil {
			// XXX can't implement panic metrics without introducing unnecessary complexity, and this code is already excessively complex. don't hesitate to remove the panic metrics in MakeBroadcastRequest if they appear to be asymmetrical.
			mylog.LogPanic(zerolog.Ctx(ctx).Error(), reason).Uint("shard", shard).Stringer("queued", endpoint.RemoteAddr()).Msg("panic in MakeMulticastRequest")

			errChan <- fmt.Errorf("shard #%d (%s): %w", shard, endpoint.RemoteAddr().String(), ErrPanic)
		}

		wg.Done()
	}()

	for start := 0; start < len(ids); start += batchSize {
		end := start + batchSize
		if end > len(ids) {
			end = len(ids)
		}

		batch := ids[start:end]
		req := reqFunc(batch)

		respBytes, op, err := qc.reqMultiRetryLoop(ctx, shard, endpoint, req)
		if err != nil {
			// TODO логгировать номера ивентов в том же формате, как и везде - будет ли админам польза от этого?
			errChan <- fmt.Errorf("shard #%d (%s), event_ids=%v: %s%w", shard, endpoint.RemoteAddr().String(), batch, op, err)
			continue
		}

		if resFunc == nil || len(respBytes) == 0 {
			continue
		}

		if err = resFunc(shard, respBytes); err != nil {
			errChan <- fmt.Errorf("shard #%d (%s), event_ids=%v: ResultFunc: %w", shard, endpoint.RemoteAddr().String(), batch, err)
		}
	}
}

func (qc *QueuedClient) reqMultiRetryLoop(ctx context.Context, shard uint, endpoint *easygo.Pool, req queuedproto.Request) (respBytes []byte, op string, err error) {
	reqBytes, err := req.MarshalIProto(internal.AllocBytes())
	if err != nil {
		return nil, "Pack: ", err
	}

	zLog := zerolog.Ctx(ctx)
	cmd := req.Cmd()
	queueID := 0
	retry := 0
	retryCount := qc.Config.GetInt(ConfRetryCount, DefaultRetryCount)

	if qID, ok := req.(withQueueID); ok {
		queueID = int(qID.QueueID())
	}

	for {
		zLog.Trace().
			Uint("shard", shard). // перфекционистам - ip шарда не логгирую, т.к. это не для админов, а для разработчиков
			Int("retry", retry).
			Int("retry_count", retryCount).
			Stringer("cmd", cmd).
			Interface("req", req).
			Msg("iproto.Call")

		var (
			reqStarted time.Time
			inProgress *metrics.Gauge
		)

		if (qc.metrics != advmetricsset.Set{}) {
			reqStarted = time.Now()
			inProgress = qc.metrics.Name("tp_queued_in_progress").Str("queued", endpoint.RemoteAddr().String()).Str("cmd", cmd.String()).Int("queue_id", queueID).GetOrCreateGauge(nil)

			qc.metrics.Name("tp_queued_total").Str("queued", endpoint.RemoteAddr().String()).Str("cmd", cmd.String()).Int("queue_id", queueID).GetOrCreateCounter().Inc()
			inProgress.Inc()
		}

		respBytes, err = endpoint.Call(ctx, uint32(cmd), reqBytes)

		if inProgress != nil {
			inProgress.Dec()
			qc.metrics.Name("tp_queued_duration_seconds").Str("queued", endpoint.RemoteAddr().String()).Str("cmd", cmd.String()).Int("queue_id", queueID).GetOrCreateHistogram().UpdateDuration(reqStarted)
		}

		if err == nil {
			op = ""

			zLog.Trace().Uint("shard", shard).Stringer("cmd", cmd).Bytes("resp", respBytes).Msg("iproto.Call")

			if len(respBytes) == 0 {
				return nil, "", ErrEmptyResponse
			}

			if err = queuedproto.ErrorByRetCode(respBytes[0]); err == nil || queuedproto.IsSoftError(err) {
				return respBytes[1:], "", err
			}

			if inProgress != nil {
				// err is low-cardinality
				qc.metrics.Name("tp_queued_errors").Str("queued", endpoint.RemoteAddr().String()).Str("cmd", cmd.String()).Int("queue_id", queueID).Str("err", err.Error()).GetOrCreateCounter().Inc()
			}
		} else if inProgress != nil {
			qc.metrics.Name("tp_queued_errors").Str("queued", endpoint.RemoteAddr().String()).Str("cmd", cmd.String()).Int("queue_id", queueID).Str("err", "network").GetOrCreateCounter().Inc()

			op = "Call: "
		}

		retry++

		zlEv := zLog.Err(err).
			Uint("shard", shard).
			Stringer("queued", endpoint.RemoteAddr()).
			Int("retry", retry).
			Int("retry_count", retryCount).
			Stringer("cmd", cmd).
			Interface("req", req)

		if retry >= retryCount {
			zlEv.Msg("retry count exceeded")
			return nil, op, err
		}

		retryDelay := qc.Config.GetDuration(ctx, ConfRetryDelay, DefaultRetryDelay)

		zlEv.Dur("retry_delay", retryDelay).Msg("retry on error")
		time.Sleep(retryDelay)
	}
}

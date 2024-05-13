package queuedclient

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"gitlab.corp.mail.ru/mygo/easygo.git/iproto"
	"gitlab.corp.mail.ru/mygo/queuedproto.git"
	"gitlab.corp.mail.ru/mygo/utils.git/mylog"
)

type (
	// RequestFunc вызывается функцией MakeMulticastRequest для формирования запроса для указанных ID (относящихся к одному шарду)
	RequestFunc func(eventIDs []uint64) queuedproto.Request

	// ResultFunc вызывается функциями MakeBroadcastRequest и MakeMulticastRequest для сохранения результата, полученного из указанного шарда.
	// Результат надо сохранять либо через замыкание - либо передавать в качестве этой функции bound method объекта, куда сохраняются данные.
	//
	// Эта функция вызывается из разных горутин и должна быть thread-safe (не использовать прямой append/обычный map без лока! использовать каналы, sync.Map и т.д.)
	ResultFunc func(shard uint, result []byte) error
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
// Если задан shard == BalanceShard, шард выбирается по round robin. В этом случае, каждый ретрай посылается в следующий шард.
func (qc *QueuedClient) MakeUnicastRequest(ctx context.Context, shard uint, req queuedproto.Request) (usedShard uint, respBytes []byte, err error) {
	if shard != BalanceShard && shard >= qc.ShardNum() {
		return 0, nil, fmt.Errorf("%w %d", ErrInvalidShard, shard)
	}

	reqBytes, err := iproto.Pack(req)

	if err != nil {
		return 0, nil, err
	}

	cmd := req.Cmd()
	retry := 0

	for {
		usedShard = qc.balanceShard(shard)
		endpoint := qc.shards[usedShard]
		respBytes, err = endpoint.Call(ctx, cmd, reqBytes)

		if err == nil {
			if len(respBytes) == 0 {
				return usedShard, nil, ErrEmptyResponse
			}

			if err = queuedproto.ErrorByRetCode(respBytes[0]); err == nil || queuedproto.IsSoftError(err) {
				return usedShard, respBytes[1:], err
			}
		}

		retry++

		retryCount := qc.Config.GetInt(ConfRetryCount, DefaultRetryCount)

		zlEv := zerolog.Ctx(ctx).Err(err).
			Uint("shard", usedShard).
			Stringer("queued", endpoint.RemoteAddr()).
			Int("retry", retry).
			Int("retry_count", retryCount).
			Uint32("cmd", cmd).
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
// Возможно, использовать общее с box решение, после перевода его на easygo/iproto.
func (qc *QueuedClient) MakeBroadcastRequest(ctx context.Context, req queuedproto.Request, resFunc ResultFunc) error {
	reqBytes, err := iproto.Pack(req)
	if err != nil {
		return err
	}

	cmd := req.Cmd()
	errChan := make(chan error)
	errs := make(ErrMulti, 0, len(qc.shards))

	for shard, endpoint := range qc.shards {
		go func(shard uint, endpoint *iproto.Pool) {
			var err error

			defer func() {
				if reason := recover(); reason != nil {
					mylog.LogPanic(zerolog.Ctx(ctx).Error(), reason).Uint("shard", shard).Stringer("queued", endpoint.RemoteAddr()).Msg("panic in MakeBroadcastRequest")

					err = ErrPanic
				}

				if err == nil {
					errChan <- nil
				} else {
					errChan <- fmt.Errorf("shard #%d (%s): %w", shard, endpoint.RemoteAddr(), err)
				}
			}()

			zerolog.Ctx(ctx).Trace().Uint("shard", shard).Uint32("cmd", cmd).Interface("req", req).Msg("iproto.Call")

			respBytes, err := endpoint.Call(ctx, cmd, reqBytes)
			if err != nil {
				return
			}

			zerolog.Ctx(ctx).Trace().Uint("shard", shard).Uint32("cmd", cmd).Bytes("resp", respBytes).Msg("iproto.Call")

			if len(respBytes) == 0 {
				err = ErrEmptyResponse
				return
			}

			if err = queuedproto.ErrorByRetCode(respBytes[0]); err != nil {
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
			mylog.LogPanic(zerolog.Ctx(ctx).Error(), reason).Uint("shard", shard).Stringer("queued", endpoint.RemoteAddr()).Msg("panic in MakeMulticastRequest")
			errChan <- fmt.Errorf("shard #%d (%s): %w", shard, endpoint.RemoteAddr(), ErrPanic)
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
			errChan <- fmt.Errorf("shard #%d (%s), event_ids=%v: %s%w", shard, endpoint.RemoteAddr(), batch, op, err)
			continue
		}

		if resFunc == nil || len(respBytes) == 0 {
			continue
		}

		if err = resFunc(shard, respBytes); err != nil {
			errChan <- fmt.Errorf("shard #%d (%s), event_ids=%v: ResultFunc: %w", shard, endpoint.RemoteAddr(), batch, err)
		}
	}
}

func (qc *QueuedClient) reqMultiRetryLoop(ctx context.Context, shard uint, endpoint *iproto.Pool, req queuedproto.Request) (respBytes []byte, op string, err error) {
	reqBytes, err := iproto.Pack(req)
	if err != nil {
		return nil, "Pack: ", err
	}

	zLog := zerolog.Ctx(ctx)
	cmd := req.Cmd()
	retry := 0
	retryCount := qc.Config.GetInt(ConfRetryCount, DefaultRetryCount)

	for {
		zLog.Trace().
			Uint("shard", shard). // перфекционистам - ip шарда не логгирую, т.к. это не для админов, а для разработчиков
			Int("retry", retry).
			Int("retry_count", retryCount).
			Uint32("cmd", cmd).
			Interface("req", req).
			Msg("iproto.Call")

		respBytes, err = endpoint.Call(ctx, req.Cmd(), reqBytes)

		if err == nil {
			op = ""

			zLog.Trace().Uint("shard", shard).Uint32("cmd", cmd).Bytes("resp", respBytes).Msg("iproto.Call")

			if len(respBytes) == 0 {
				return nil, "", ErrEmptyResponse
			}

			if err = queuedproto.ErrorByRetCode(respBytes[0]); err == nil || queuedproto.IsSoftError(err) {
				return respBytes[1:], "", err
			}
		} else {
			op = "Call: "
		}

		retry++

		zlEv := zLog.Err(err).
			Uint("shard", shard).
			Stringer("queued", endpoint.RemoteAddr()).
			Int("retry", retry).
			Int("retry_count", retryCount).
			Uint32("cmd", cmd).
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

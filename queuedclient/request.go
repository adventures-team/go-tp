package queuedclient

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	advslog "github.com/adventures-team/go-adv-slog"
	iprotoconn "github.com/adventures-team/go-iprotoconn"
	iprotoserver "github.com/adventures-team/go-iprotoconn/server"
	"github.com/adventures-team/go-queuedproto"

	"gitlab.adv.team/go/tp.git/internal"
)

type (
	// RequestFunc вызывается функцией [QueuedClient.MakeMulticastRequest] для формирования запроса для указанных ID (относящихся к одному шарду)
	RequestFunc func(eventIDs []uint64) queuedproto.Request

	// ResultFunc вызывается функциями [QueuedClient.MakeBroadcastRequest] и [QueuedClient.MakeMulticastRequest] для сохранения результата, полученного из указанного шарда.
	// Результат надо сохранять либо через замыкание - либо передавать в качестве этой функции bound method объекта, куда сохраняются данные.
	//
	// Эта функция вызывается из разных горутин и должна быть thread-safe (не использовать прямой append/обычный map без лока! использовать каналы, [sync.Map] и т.д.)
	ResultFunc func(shard uint, result []byte) error
)

func (qc *QueuedClient) balanceShard(shard uint) uint {
	if shard != BalanceShard {
		return shard
	}

	return uint(atomic.AddUint32(&qc.roundRobin, 1)) % qc.ShardNum()
}

// hardRetCode запоминает последний "жёсткий" код возврата, увиденный классификатором
// в рамках одного вызова: движок не возвращает тело ответа неисправного сервера,
// а вызывающие стороны матчат протокольные ошибки (errors.Is / текст) - поэтому
// после исчерпания ретраев код возврата прикладывается к итоговой ошибке.
// Атомик - на случай параллельных попыток (hedging).
type hardRetCode struct {
	code atomic.Int32 // -1 = не было
}

// classify классифицирует ответ queued для движка ретраев go-iprotoconn:
// пустой ответ и "жёсткие" коды возврата - OutcomeHard (сервер замораживается,
// попытка перезапрашивается, ошибка учитывается circuit breaker'ом);
// успех и "мягкие" (прикладные, [queuedproto.IsSoftError]) ошибки - OutcomeOK,
// код возврата разбирается вызывающей стороной.
func (h *hardRetCode) classify(_ uint32, payload []byte) iprotoconn.Outcome {
	if len(payload) == 0 {
		return iprotoconn.OutcomeHard
	}

	if err := queuedproto.ErrorByRetCode(payload[0]); err != nil && !queuedproto.IsSoftError(err) {
		h.code.Store(int32(payload[0]))
		return iprotoconn.OutcomeHard
	}

	return iprotoconn.OutcomeOK
}

// wrap оборачивает протокольную ошибку последнего "жёсткого" кода возврата ошибкой движка
// (если такой код был): callErr остаётся в цепочке, errors.Is работает для обоих.
func (h *hardRetCode) wrap(callErr error) error {
	if rc := h.code.Load(); rc >= 0 {
		if rcErr := queuedproto.ErrorByRetCode(byte(rc)); rcErr != nil {
			return fmt.Errorf("%w (%w)", rcErr, callErr)
		}
	}

	return callErr
}

// call выполняет запрос к указанному шарду (нумерация с 0). Ретраи, задержки
// между ними и таймауты - на движке go-iprotoconn согласно /queueds/multi/*
// (см. iprotoSource); дополнительные opts могут переопределить их per call.
//
// Возвращает тело ответа без байта кода возврата. "Мягкие" ошибки возвращаются
// вместе с телом ответа (как и раньше). "Жёсткие" коды возврата, исчерпавшие
// ретраи, возвращаются протокольной ошибкой ([queuedproto.ErrorByRetCode]) с
// приложенной ошибкой движка (*iprotoconn.Error) в цепочке - errors.Is работает
// для обеих.
func (qc *QueuedClient) call(ctx context.Context, shard uint, req queuedproto.Request, opts ...iprotoconn.CallOption) ([]byte, error) {
	reqBytes, err := req.MarshalIProto(internal.AllocBytes())
	if err != nil {
		return nil, fmt.Errorf("pack: %w", err)
	}

	lastHard := &hardRetCode{}
	lastHard.code.Store(-1)

	callOpts := make([]iprotoconn.CallOption, 0, len(opts)+2)
	callOpts = append(callOpts,
		iprotoconn.WithShard(int(shard)+1), // go-iprotoconn нумерует шарды с 1
		iprotoconn.WithClassifierRaw(lastHard.classify),
	)
	callOpts = append(callOpts, opts...)

	// RawRequest обходит несовместимость интерфейсов (queuedproto.Cmd - именованный
	// тип, движку нужен Cmd() uint32) и сохраняет один маршалинг на все шарды
	// broadcast/multicast-запросов.
	var resp iprotoserver.RawResponse

	err = qc.client.Call(ctx, iprotoserver.RawRequest(uint32(req.Cmd()), reqBytes), &resp, callOpts...)
	if err != nil {
		return nil, lastHard.wrap(err)
	}

	if len(resp) == 0 {
		return nil, ErrEmptyResponse
	}

	// сюда доходят только успех и "мягкие" ошибки: "жёсткие" коды классифицированы
	// как OutcomeHard и превращаются движком в ошибку Call после ретраев
	return resp[1:], queuedproto.ErrorByRetCode(resp[0])
}

// MakeUnicastRequest посылает запрос req к указанному шарду.
//
// Ретраи при не-логических ошибках (!queuedproto.IsSoftError(err)) выполняет движок
// go-iprotoconn: /queueds/multi/retry_count попыток с задержкой /queueds/multi/retry_delay,
// каждая попытка ограничена /queueds/multi/timeout.
//
// Если задан shard == [BalanceShard], шард выбирается по round robin. Если запрос
// гарантированно не был отправлен ([iprotoconn.RequestNotExecuted] - например,
// у шарда открыт circuit breaker), пробуется следующий шард - это безопасно
// даже для неидемпотентных запросов.
func (qc *QueuedClient) MakeUnicastRequest(ctx context.Context, shard uint, req queuedproto.Request) (usedShard uint, respBytes []byte, err error) {
	if shard != BalanceShard && shard >= qc.ShardNum() {
		return 0, nil, fmt.Errorf("%w %d", ErrInvalidShard, shard)
	}

	shardsToTry := 1
	if shard == BalanceShard {
		shardsToTry = int(qc.ShardNum())
	}

	for range shardsToTry {
		usedShard = qc.balanceShard(shard)

		respBytes, err = qc.call(ctx, usedShard, req)
		if err == nil || !iprotoconn.RequestNotExecuted(err) {
			return usedShard, respBytes, err
		}

		advslog.Ctx(ctx).Warn("queued shard unavailable, trying next",
			advslog.Err(err), "shard", usedShard, "queued", qc.addrs[usedShard], "cmd", req.Cmd().String())
	}

	return usedShard, respBytes, err
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
	errChan := make(chan error)
	errs := make(ErrMulti, 0, qc.ShardNum())

	for shard := range qc.ShardNum() {
		go func(shard uint) {
			var err error

			defer func() {
				if reason := recover(); reason != nil {
					advslog.Ctx(ctx).LogAttrs(ctx, slog.LevelError, "panic in MakeBroadcastRequest",
						append(advslog.PanicAttrs(reason), slog.Uint64("shard", uint64(shard)), slog.String("queued", qc.addrs[shard]))...)

					err = ErrPanic
				}

				if err == nil {
					errChan <- nil
				} else {
					errChan <- fmt.Errorf("shard #%d (%s): %w", shard, qc.addrs[shard], err)
				}
			}()

			var respBytes []byte

			respBytes, err = qc.call(ctx, shard, req, iprotoconn.WithMaxTries(1)) // без ретраев
			if err != nil {
				return
			}

			if resFunc == nil || len(respBytes) == 0 {
				return
			}

			err = resFunc(shard, respBytes)
		}(shard)
	}

	for range qc.ShardNum() {
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
// Ретраи при не-логических ошибках (!queuedproto.IsSoftError(err)) выполняет движок
// go-iprotoconn (/queueds/multi/retry_count попыток с задержкой /queueds/multi/retry_delay);
// если ошибка осталась после всех попыток - она вносится в список ошибок шардов.
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
	errs := make(ErrMulti, 0, qc.ShardNum())
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
	defer func() {
		if reason := recover(); reason != nil {
			advslog.Ctx(ctx).LogAttrs(ctx, slog.LevelError, "panic in MakeMulticastRequest",
				append(advslog.PanicAttrs(reason), slog.Uint64("shard", uint64(shard)), slog.String("queued", qc.addrs[shard]))...)

			errChan <- fmt.Errorf("shard #%d (%s): %w", shard, qc.addrs[shard], ErrPanic)
		}

		wg.Done()
	}()

	for start := 0; start < len(ids); start += batchSize {
		end := min(start+batchSize, len(ids))

		batch := ids[start:end]
		req := reqFunc(batch)

		respBytes, err := qc.call(ctx, shard, req)
		if err != nil {
			// TODO логгировать номера ивентов в том же формате, как и везде - будет ли админам польза от этого?
			errChan <- fmt.Errorf("shard #%d (%s), event_ids=%v: %w", shard, qc.addrs[shard], batch, err)
			continue
		}

		if resFunc == nil || len(respBytes) == 0 {
			continue
		}

		if err = resFunc(shard, respBytes); err != nil {
			errChan <- fmt.Errorf("shard #%d (%s), event_ids=%v: ResultFunc: %w", shard, qc.addrs[shard], batch, err)
		}
	}
}

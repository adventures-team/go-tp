package queuedclient

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"

	"gitlab.corp.mail.ru/e-mail-ru/easygo/iproto"
	"gitlab.corp.mail.ru/mygo/queuedproto.git"
	"gitlab.corp.mail.ru/mygo/utils.git/mylog"
)

type (
	RequestFunc func(eventIDs []uint64) queuedproto.Request
	ResultFunc  func(shard uint, result []byte) error
)

func (qc *QueuedClient) balanceShard(shard uint) uint {
	if shard != BalanceShard {
		return shard
	}

	return uint(atomic.AddUint32(&qc.roundRobin, 1)) % qc.ShardNum()
}

func (qc *QueuedClient) MakeUnicastRequest(ctx context.Context, shard uint, req queuedproto.Request) ([]byte, error) {
	reqBytes, err := iproto.Pack(req)
	if err != nil {
		return nil, err
	}

	if shard >= qc.ShardNum() {
		return nil, fmt.Errorf("%w %d", ErrInvalidShard, shard)
	}

	respBytes, err := qc.shards[shard].Call(ctx, req.Cmd(), reqBytes)
	if err != nil {
		return nil, err
	}

	if len(respBytes) == 0 {
		return nil, ErrEmptyResponse
	}

	return respBytes[1:], queuedproto.ErrorByRetCode(respBytes[0])
}

// TODO раскопипастить broad/multicast запросы - когда полностью определимся с их логикой.

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

			respBytes, err := endpoint.Call(ctx, cmd, reqBytes)
			if err != nil {
				return
			}

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
		reqBytes, err := iproto.Pack(req)

		// TODO логгировать номера ивентов в том же формате, как и везде - будет ли админам польза от этого?

		if err != nil {
			errChan <- fmt.Errorf("shard #%d (%s), event_ids=%v: Pack: %w", shard, endpoint.RemoteAddr(), batch, err)
			continue
		}

		cmd := req.Cmd()

		zerolog.Ctx(ctx).Trace().Uint("shard", shard).Uint32("cmd", cmd).Interface("req", req).Msg("iproto.Call")

		respBytes, err := endpoint.Call(ctx, req.Cmd(), reqBytes)
		if err != nil {
			errChan <- fmt.Errorf("shard #%d (%s), event_ids=%v: Call: %w", shard, endpoint.RemoteAddr(), batch, err)
			continue
		}

		zerolog.Ctx(ctx).Trace().Uint("shard", shard).Uint32("cmd", cmd).Bytes("resp", respBytes).Msg("iproto.Call")

		if len(respBytes) == 0 {
			errChan <- fmt.Errorf("shard #%d (%s), event_ids=%v: %w", shard, endpoint.RemoteAddr(), batch, ErrEmptyResponse)
			continue
		}

		if err = queuedproto.ErrorByRetCode(respBytes[0]); err != nil {
			errChan <- fmt.Errorf("shard #%d (%s), event_ids=%v: %w", shard, endpoint.RemoteAddr(), batch, err)
			continue
		}

		if resFunc == nil || len(respBytes) == 1 {
			continue
		}

		if err = resFunc(shard, respBytes[1:]); err != nil {
			errChan <- fmt.Errorf("shard #%d (%s), event_ids=%v: ResultFunc: %w", shard, endpoint.RemoteAddr(), batch, err)
		}
	}
}

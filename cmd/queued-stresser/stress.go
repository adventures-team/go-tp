package main

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/my-mail-ru/go-queuedproto"

	easygo "gitlab.adv.team/go/easygo.git/iproto"
	"gitlab.adv.team/go/tp.git/internal"
)

// ConnPool represents stress test connection pool state.
type ConnPool struct {
	pid         uint32
	reqID       atomic.Uint32
	bytesSent   atomic.Uint64
	queueID     uint16
	connections int
	pipeline    int
	master      string
	timeout     time.Duration
	durationMu  sync.Mutex
	duration    time.Duration
	durationMax time.Duration
	delay       atomic.Uint64
	logger      easygo.Logger
	payload     []byte // using fixed payload since queued doesn't implement data compression
}

// NewConnPool initializes ConnPool. Each new pool comes with the unique random pid.
func NewConnPool(opt *Options) *ConnPool {
	payload := make([]byte, opt.Size)
	_, _ = rand.Read(payload) //nolint

	cp := &ConnPool{
		pid:         uint32(rand.Intn(99999)), //nolint:gosec
		connections: opt.Connections,
		pipeline:    opt.Pipeline,
		master:      opt.MasterPort,
		timeout:     opt.Timeout,
		queueID:     uint16(opt.QueueID),
		logger:      dumbLogger{},
		payload:     payload,
	}

	if opt.Debug {
		cp.logger = debugLogger{}
	}

	return cp
}

// TotalCount atomically reads current request ID (i.e. total request count).
func (cp *ConnPool) TotalCount() uint32 {
	return cp.reqID.Load()
}

// SampleSize returns the total size of all requests sent since the previous SampleSize call.
func (cp *ConnPool) SampleSize() uint64 {
	return cp.bytesSent.Swap(0)
}

// SampleDuration returns the total/maximum durations of all requests sent since the previous SampleDuration call.
func (cp *ConnPool) SampleDuration() (total time.Duration, maximum time.Duration) {
	cp.durationMu.Lock()
	duration, durationMax := cp.duration, cp.durationMax
	cp.duration, cp.durationMax = 0, 0
	cp.durationMu.Unlock()

	return duration, durationMax
}

// SetDelay sets the duration of delay performed after each request.
func (cp *ConnPool) SetDelay(delay time.Duration) {
	cp.delay.Store(uint64(delay))
}

// GetDelay returns the current duration of delay performed after each request.
func (cp *ConnPool) GetDelay() time.Duration {
	return time.Duration(cp.delay.Load())
}

// Run the connection pool until the context is cancelled.
// Connections are restarted on protocol- and connection-level errors.
func (cp *ConnPool) Run(ctx context.Context) {
	wg := sync.WaitGroup{}
	tickets := make(chan struct{}, cp.connections)

loop:
	for {
		select {
		case tickets <- struct{}{}:
		case <-ctx.Done():
			break loop
		}

		wg.Add(1)

		go func() {
			if err := cp.connLoop(ctx); err != nil {
				if ctx.Err() == nil {
					log.Printf("connection loop failed: %v", err)
				}
			}

			<-tickets
			wg.Done()
		}()
	}

	wg.Wait()
}

func (cp *ConnPool) connLoop(ctx context.Context) error {
	conn, err := easygo.Dial(ctx, "tcp", cp.master, &easygo.PoolConfig{
		Size:              1,
		ConnectTimeout:    cp.timeout,
		DialTimeout:       cp.timeout,
		RedialInterval:    redialInterval,
		MaxRedialInterval: redialInterval,
		RedialForever:     true,
		Logger:            cp.logger,
		ChannelConfig: &easygo.ChannelConfig{
			WriteTimeout:   cp.timeout,
			RequestTimeout: cp.timeout,
			PingInterval:   pingInterval,
			Logger:         cp.logger,
		},
	})
	if err != nil {
		return err
	}

	defer conn.Close()

	ctx, cancel := context.WithCancelCause(ctx)
	tickets := make(chan struct{}, cp.pipeline)

	defer cancel(nil)

loop:
	for {
		select {
		case tickets <- struct{}{}:
		case <-ctx.Done():
			break loop
		}

		go func() {
			if err := cp.makeRequest(ctx, conn); err != nil {
				cancel(err) // the first error cancels the loop
			}

			SleepContext(ctx, time.Duration(cp.delay.Load()))

			<-tickets
		}()
	}

	return context.Cause(ctx)
}

func (cp *ConnPool) makeRequest(ctx context.Context, conn *easygo.Pool) error {
	req := queuedproto.ReqAddItem{
		Pid:         cp.pid,
		ReqID:       cp.reqID.Add(1),
		StorageType: cp.queueID,
		ID:          0,
		UnixTime:    uint32(time.Now().Unix()),
		Data:        cp.payload,
		Flags:       0,
	}

	reqBytes, err := req.MarshalIProto(internal.AllocBytes())
	if err != nil {
		return err
	}

	cp.bytesSent.Add(uint64(len(reqBytes)))

	start := time.Now()

	respBytes, err := conn.Call(ctx, uint32(req.Cmd()), reqBytes)
	if err != nil {
		return err
	}

	if len(respBytes) == 0 {
		return errors.New("ReqAddItem: empty response")
	}

	reqDuration := time.Since(start)

	cp.durationMu.Lock()
	cp.duration += reqDuration

	if cp.durationMax < reqDuration {
		cp.durationMax = reqDuration
	}

	cp.durationMu.Unlock()

	return queuedproto.ErrorByRetCode(respBytes[0])
}

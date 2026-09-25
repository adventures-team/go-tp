package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adventures-team/go-iprotoconn/config"
	"github.com/adventures-team/go-iprotoconn/conn"
	"github.com/adventures-team/go-queuedproto"

	"github.com/adventures-team/go-tp/internal"
)

// ConnPool represents stress test connection pool state.
//
// Connections are built directly on go-iprotoconn's conn layer: the cluster
// client deduplicates connections per address process-wide, while a load
// generator needs -conn real TCP connections to the same queued.
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
	payload     []byte // using fixed payload since queued doesn't implement data compression
}

// stressSource is a static go-iprotoconn config source for the conn layer.
// Cancels are disabled: legacy queued closes the connection on an unknown
// control message type (verified against a real queued; see
// TestCancelDoesNotKillConnection).
type stressSource struct {
	timeout     time.Duration
	maxInFlight int
}

func (s stressSource) GetDuration(path string, dflt time.Duration) time.Duration {
	switch path {
	case "/write-timeout":
		return s.timeout
	case "/ping-interval":
		return pingInterval
	}

	return dflt
}

func (s stressSource) GetInt(path string, dflt int) int {
	if path == "/max-in-flight" {
		return s.maxInFlight
	}

	return dflt
}

func (stressSource) GetBool(path string, dflt bool) bool {
	if path == "/cancels-enabled" {
		return false
	}

	return dflt
}

func (stressSource) GetString(_ string, dflt string) string      { return dflt }
func (stressSource) GetStringIfExists(_ string) (string, bool)   { return "", false }
func (stressSource) GetFloat(_ string, dflt float64) float64     { return dflt }
func (stressSource) GetStrings(_ string, dflt []string) []string { return dflt }

// NewConnPool initializes ConnPool. Each new pool comes with the unique random pid.
func NewConnPool(opt *Options) *ConnPool {
	payload := make([]byte, opt.Size)
	_, _ = rand.Read(payload) //nolint

	return &ConnPool{
		pid:         uint32(rand.Intn(99999)), //nolint:gosec
		connections: opt.Connections,
		pipeline:    opt.Pipeline,
		master:      opt.MasterPort,
		timeout:     opt.Timeout,
		queueID:     uint16(opt.QueueID),
		payload:     payload,
	}
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
	dialer := net.Dialer{Timeout: cp.timeout}

	nc, err := dialer.DialContext(ctx, "tcp", cp.master)
	if err != nil {
		return err
	}

	cn := conn.New(nc, conn.Options{
		Config: config.ForConn(stressSource{timeout: cp.timeout, maxInFlight: cp.pipeline * 2}),
		Logger: slog.Default(),
		Role:   conn.RoleDialer,
	})

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	go func() {
		select {
		case <-cn.Closed():
			cancel(errors.New("connection closed"))
		case <-ctx.Done():
			cn.Drop(context.Cause(ctx))
		}
	}()

	tickets := make(chan struct{}, cp.pipeline)

loop:
	for {
		select {
		case tickets <- struct{}{}:
		case <-ctx.Done():
			break loop
		}

		go func() {
			if err := cp.makeRequest(ctx, cn); err != nil {
				cancel(err) // the first error cancels the loop
			}

			SleepContext(ctx, time.Duration(cp.delay.Load()))

			<-tickets
		}()
	}

	return context.Cause(ctx)
}

func (cp *ConnPool) makeRequest(ctx context.Context, cn *conn.Conn) error {
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

	reqCtx, cancelReq := context.WithTimeout(ctx, cp.timeout)
	defer cancelReq()

	start := time.Now()

	respBytes, err := cn.Invoke(reqCtx, uint32(req.Cmd()), reqBytes)
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

package qmclient

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adventures-team/go-iprotoconn/config"
	"github.com/adventures-team/go-iprotoconn/conn"

	"gitlab.adv.team/go/tp.git/internal"
)

// testSource - config.Source, отдающий сплошные дефолты (для conn-стороны фейкового
// менеджера очередей).
type testSource struct{}

func (testSource) GetString(_ string, dflt string) string                 { return dflt }
func (testSource) GetStringIfExists(_ string) (string, bool)              { return "", false }
func (testSource) GetInt(_ string, dflt int) int                          { return dflt }
func (testSource) GetBool(_ string, dflt bool) bool                       { return dflt }
func (testSource) GetDuration(_ string, dflt time.Duration) time.Duration { return dflt }
func (testSource) GetFloat(_ string, dflt float64) float64                { return dflt }
func (testSource) GetStrings(_ string, dflt []string) []string            { return dflt }

// TestRunReconnect - сквозной тест обратного RPC без докера: фейковый менеджер
// очередей принимает соединение qmclient (поднятое eager-dial'ом сразу после Run),
// шлёт команды как peer-запросы, рвёт соединение и убеждается, что клиент
// переподключился и продолжает отвечать.
func TestRunReconnect(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = ln.Close() }()

	var assignCalls atomic.Int32

	statsData := StatsData{PID: 42, TotalSlots: 7}
	client := NewClient(ln.Addr().String(),
		func(_, _ uint32) error { assignCalls.Add(1); return nil },
		func() StatsData { return statsData },
	)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})

	go func() {
		defer close(runDone)

		client.Run(ctx)
	}()

	acceptConn := func(what string) *conn.Conn {
		t.Helper()

		if dl, ok := ln.(*net.TCPListener); ok {
			_ = dl.SetDeadline(time.Now().Add(30 * time.Second))
		}

		nc, err := ln.Accept()
		if err != nil {
			t.Fatalf("no connection from qmclient (%s): %v", what, err)
		}

		return conn.New(nc, conn.Options{
			Config: config.ForConn(testSource{}),
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
	}

	requestStats := func(qm *conn.Conn, what string) {
		t.Helper()

		reqCtx, cancelReq := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelReq()

		respBytes, err := qm.Invoke(reqCtx, cmdStat, nil)
		if err != nil {
			t.Fatalf("cmdStat (%s): %v", what, err)
		}

		var resp StatsResponse
		if _, err := resp.UnmarshalIProto(respBytes); err != nil {
			t.Fatalf("cmdStat (%s): unmarshal: %v", what, err)
		}

		if resp.Status != answerOK || resp.Cluster != cluster || resp.Data.PID != statsData.PID {
			t.Fatalf("cmdStat (%s): unexpected response: %+v", what, resp)
		}
	}

	// соединение поднимается eager-dial'ом без единого запроса со стороны клиента
	qm := acceptConn("eager dial after Run")
	requestStats(qm, "first connection")

	// "рестарт менеджера": рвём соединение, eager redial должен переподключиться
	qm.Drop(errors.New("test: queue manager restart"))

	qm = acceptConn("redial after drop")
	requestStats(qm, "after reconnect")

	// и команды с ответной статистикой работают по новому соединению
	reqBytes, err := SetQueueRequest{Queues: []SetQueue{{Slot: 1, QueueID: 5}, {Slot: 2, QueueID: 6}}}.
		MarshalIProto(internal.AllocBytes())
	if err != nil {
		t.Fatal(err)
	}

	reqCtx, cancelReq := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelReq()

	respBytes, err := qm.Invoke(reqCtx, cmdSetQueue, reqBytes)
	if err != nil {
		t.Fatalf("cmdSetQueue after reconnect: %v", err)
	}

	var setResp SetQueueResponse
	if _, err := setResp.UnmarshalIProto(respBytes); err != nil {
		t.Fatal(err)
	}

	if len(setResp.SetQueueStatuses) != 2 || setResp.SetQueueStatuses[0] != answerOK {
		t.Fatalf("unexpected SetQueueResponse: %+v", setResp)
	}

	if got := assignCalls.Load(); got != 2 {
		t.Fatalf("assignSlot calls: got %d, want 2", got)
	}

	cancel()

	select {
	case <-runDone:
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

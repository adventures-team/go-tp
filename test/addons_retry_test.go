//go:build integration

package tptest

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	advslog "github.com/adventures-team/go-adv-slog"
	"github.com/adventures-team/go-queuedproto/addons"

	"gitlab.adv.team/go/tp.git"
	"gitlab.adv.team/go/tp.git/queuedclient"
)

//adv:iproto:
type retryEvent struct {
	tp.BaseEvent
	addons.Retryable
}

func (retryEvent) Version() uint8 {
	return 1
}

func (retryEvent) QueueID() uint16 {
	return testRetryEventQueueID
}

var _ tp.Event = &retryEvent{}

func TestAddonsRetry(t *testing.T) {
	setTestLogTime()

	qm := tp.InitProcessor(
		newMockConf(t, testRetryEventQueueID),
		tp.WithInstanceName(testInstanceName),
		tp.WithLogger(advslog.InitTest(t)),
	)

	slog.Info("test starting", "pid", os.Getpid())

	handler := retryEventHandler{
		stopper: qm,
	}

	err := tp.Register(qm.Queue, tp.WithRetry(handler.handle))
	if err != nil {
		advslog.Fatal("Queue.Register", advslog.Err(err))
	}

	ctx := context.Background()

	eventID, err := qm.Publish(ctx, &retryEvent{}, queuedclient.PublishActivationTime(time.Now().Add(time.Second)))
	if err != nil {
		slog.Error("Publish", advslog.Err(err))
		panic(err)
	}

	slog.Info("published", "event_id", eventID)

	/*
		go func() { // keep queued busy
			for {
				eventID, err := qm.Publish(ctx, &retryEvent{}) //, queuedclient.PublishInitialDelay(time.Hour))
				if err != nil {
					slog.Error("qm.Publish", advslog.Err(err))
				}

				qm.Client().Delete(ctx, testRetryEventQueueID, []queuedproto.EventID{eventID})
				time.Sleep(time.Second)
			}
		}()
	*/

	qm.Run()
}

type retryEventHandler struct {
	stopper interface{ Stop() }
}

func (h retryEventHandler) handle(ctx context.Context, events []retryEvent) error {
	needStop := false

	for _, event := range events {
		count := event.GetRetryCount()
		advslog.Ctx(ctx).Info("retrying", "event_id", event.EventID, "retry_count", count)

		if count < 5 {
			event.Retry()
		} else {
			event.Done()

			needStop = true
		}
	}

	if needStop {
		h.stopper.Stop()
	}

	return nil
}

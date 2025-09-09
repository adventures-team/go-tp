//go:build integration

package tptest

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	zLog "github.com/rs/zerolog/log"

	"github.com/my-mail-ru/go-queuedproto/addons"

	"gitlab.cebc.tools/go/tp.git"
	"gitlab.cebc.tools/go/tp.git/queuedclient"
	"gitlab.cebc.tools/go/utils.git/mylog"
)

//adv:iproto:
type retryEvent struct {
	tp.BaseEvent
	addons.Retry
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

	conf := newMockConf(t, testRetryEventQueueID)

	qm := tp.Config{
		Config:       conf,
		InstanceName: "test",
		Logger:       mylog.InitTestZerolog(t),
	}.InitProcessor()

	zLog.Info().Int("pid", os.Getpid()).Send()

	handler := retryEventHandler{
		stopper: qm,
	}

	err := tp.Register(qm.Queue, tp.WithRetry(handler.handle))
	if err != nil {
		zLog.Fatal().Err(err).Msg("Queue.Register")
	}

	ctx := context.Background()

	eventID, err := qm.Publish(ctx, &retryEvent{}, queuedclient.PublishActivationTime(time.Now().Add(time.Second)))
	if err != nil {
		zLog.Panic().Err(err).Msg("Publish")
	}

	zLog.Info().Stringer("event_id", eventID).Msg("published")

	/*
		go func() { // keep queued busy
			for {
				eventID, err := qm.Publish(ctx, &retryEvent{}) //, queuedclient.PublishInitialDelay(time.Hour))
				if err != nil {
					zLog.Err(err).Msg("qm.Publish")
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
		zerolog.Ctx(ctx).Info().Stringer("event_id", event.EventID).Uint32("retry_count", count).Send()

		if count < 5 {
			event.Fail()
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

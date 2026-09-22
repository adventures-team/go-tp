//go:build integration

package tptest

import (
	"context"
	"testing"

	advslog "github.com/adventures-team/go-adv-slog"

	"github.com/onlineconf/onlineconf-go/v2"

	"gitlab.adv.team/go/tp.git"
	"gitlab.adv.team/go/tp.git/queueconfig"
	"gitlab.adv.team/go/tp.git/workerpool"
)

func TestQueueWorker(t *testing.T) {
	logger := advslog.InitTest(t)
	ctx := advslog.NewContext(context.Background(), logger)

	conf, err := onlineconf.OpenSubtree(testCDB(t), testConfPrefix)
	if err != nil {
		t.Fatal(err)
	}

	queueConf := queueconfig.New(conf, testInstanceName)

	queue, err := tp.NewQueue(ctx, queueConf)
	if err != nil {
		t.Fatal("error initializing queue processor:", err)
	}

	cnt := newCounter(1) // 1 - событие, посылаемое вне цикла
	svc := testService{Counter: cnt}

	err = tp.Register(queue, svc.HandleEventWithAddons)
	if err != nil {
		t.Fatal("Register failed:", err)
	}

	err = tp.Register(queue, svc.HandleEventWithAddonsV2)
	if err != nil {
		t.Fatal("Register failed:", err)
	}

	pool := workerpool.New(queue.NewPoller(logger), logger)

	pool.SetWorkerCount(testAddonsQueueID, 5)

	ev := &eventWithAddons{
		TrackID: 1,
		ExternalID:   2,
	}

	evV2 := &eventWithAddonsV2{
		TrackID: 11,
		ExternalID:   22,
	}

	eventID, err := queue.Publish(ctx, ev)
	if err != nil {
		t.Fatalf("Publish(%v) failed: %v", ev, err)
	}

	logger.Info("event published", "event_id", eventID)

	eventID, err = queue.Publish(ctx, ev)
	if err != nil {
		t.Fatalf("Publish(%v) failed with IgnoreDuplicateItems set: %v", ev, err)
	}

	logger.Info("event published (duplicate)", "event_id", eventID)

	for i := 0; i < 7; i++ {
		cnt.Add(2)

		ev.TrackID++
		ev.ExternalID++

		eventID, err = queue.Publish(ctx, ev)
		if err != nil {
			t.Fatalf("Publish(%v) failed set: %v", ev, err)
		}

		logger.Info("event published", "event_id", eventID)

		eventID, err = queue.Publish(ctx, evV2)
		if err != nil {
			t.Fatalf("Publish(%v) failed set: %v", ev, err)
		}

		logger.Info("V2 event published", "event_id", eventID)

		evV2.TrackID++
		evV2.ExternalID++
	}

	go func() {
		cnt.Wait()
		pool.Stop()
	}()

	pool.Run() // тест на то, что Manage не выйдет раньше вызова pool.Stop. если запускать в горутине его, а в основной ждать doneChan, это будет неочевидно
}

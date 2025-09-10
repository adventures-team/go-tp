//go:build integration

package tptest

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/onlineconf/onlineconf-go/v2"

	"gitlab.cebc.tools/go/tp.git"
	"gitlab.cebc.tools/go/tp.git/queueconfig"
	"gitlab.cebc.tools/go/tp.git/workerpool"
	"gitlab.cebc.tools/go/utils.git/mylog"
)

func TestQueueWorker(t *testing.T) {
	logger := mylog.InitTestZerolog(t)
	ctx := logger.WithContext(context.Background())

	conf, err := onlineconf.OpenSubtree(filepath.Join(ConfigDir(), "TREE.cdb"), testConfPrefix)
	if err != nil {
		t.Fatal(err)
	}

	queueConf := queueconfig.New(conf, testInstanceName)

	queue, err := tp.Init(ctx, queueConf)
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

	logger.Info().Msgf("event_id=%v published", eventID)

	eventID, err = queue.Publish(ctx, ev)
	if err != nil {
		t.Fatalf("Publish(%v) failed with IgnoreDuplicateItems set: %v", ev, err)
	}

	logger.Info().Msgf("event_id=%v published (duplicate)", eventID)

	for i := 0; i < 7; i++ {
		ev.TrackID++
		ev.ExternalID++

		eventID, err = queue.Publish(ctx, ev)
		if err != nil {
			t.Fatalf("Publish(%v) failed set: %v", ev, err)
		}

		logger.Info().Msgf("event_id=%v published", eventID)

		eventID, err = queue.Publish(ctx, evV2)
		if err != nil {
			t.Fatalf("Publish(%v) failed set: %v", ev, err)
		}

		logger.Info().Msgf("V2 event_id=%v published", eventID)

		evV2.TrackID++
		evV2.ExternalID++

		cnt.Add(2)
	}

	go func() {
		cnt.Wait()
		pool.Stop()
	}()

	pool.Run() // тест на то, что Manage не выйдет раньше вызова pool.Stop. если запускать в горутине его, а в основной ждать doneChan, это будет неочевидно
}

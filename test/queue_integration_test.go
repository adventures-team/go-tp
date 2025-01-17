//go:build integration
// +build integration

package tptest

import (
	"context"
	"testing"

	"github.com/onlineconf/onlineconf-go"

	"gitlab.corp.my.games/adventures/go/tp.git"
	"gitlab.corp.my.games/adventures/go/tp.git/queueconfig"
	"gitlab.corp.my.games/adventures/go/tp.git/workerpool"
	"gitlab.corp.my.games/adventures/go/utils.git/mylog"
	"gitlab.corp.my.games/adventures/go/utils.git/oconf"
)

func TestQueueWorker(t *testing.T) {
	logger := mylog.InitTestZerolog(t)
	ctx := logger.WithContext(context.Background())

	onlineconf.Initialize(ConfigDir())
	conf := oconf.NewSubtree(onlineconf.GetModule("TREE"), testConfPrefix)
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

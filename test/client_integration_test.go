//go:build integration

package tptest

import (
	"bytes"
	"context"
	"math/rand"
	"sort"
	"testing"
	"time"

	advslog "github.com/adventures-team/go-adv-slog"

	"github.com/adventures-team/go-queuedproto"

	"github.com/adventures-team/go-tp/queuedclient"
)

// Тест только для queuedclient, безо всей высокоуровневой логики. Копипастить/изучать этот файл не надо (если не пишите какую-нибудь низкоуровневую тулзу для фиксов).
// В любой непонятной ситуации - рестартим докер с тестовым куедом (т.е. чистим его).

type eventDataMap map[queuedproto.EventID][]byte

func TestSingle(t *testing.T) {
	testData := []byte("test event")

	ctx, client := getTestClient(t)

	eventID, err := client.Publish(ctx, testRawQueueID, queuedproto.EventID{Shard: queuedclient.BalanceShard, ID: 0}, 0, testData)
	if err != nil {
		t.Fatal(err)
	}

	events, err := client.GetByIDs(ctx, testRawQueueID, []queuedproto.EventID{eventID})
	if err != nil {
		t.Fatalf("GetByIDs(%v): %v", eventID, err)
	}

	if len(events.Items) != 1 {
		t.Fatalf("strange number of items received: %#v", events)
	}

	if !bytes.Equal(events.Items[0].Data, testData) {
		t.Fatalf("testData: got: %s, expected: %s", events.Items[0].Data, testData)
	}

	if err = client.Delete(ctx, testRawQueueID, []queuedproto.EventID{eventID}); err != nil {
		t.Fatal(err)
	}
}

func TestBatch(t *testing.T) {
	ctx, client := getTestClient(t)
	zlog := advslog.Ctx(ctx)
	byID := eventDataMap{}

	for i := 0; i < testBatchSize; i++ {
		testData := make([]byte, 10)

		_, err := rand.Read(testData) //nolint
		if err != nil {
			t.Fatalf("rand.Read: %v", err)
		}

		eventID, err := client.Publish(ctx, testRawQueueID, queuedproto.EventID{Shard: queuedclient.BalanceShard, ID: 0}, 0, testData)
		if err != nil {
			t.Fatal(err)
		}

		zlog.Info("event published", "eventID", eventID)

		if rand.Intn(2) == 1 {
			err = client.Defer(ctx, testRawQueueID, testDefer, []queuedproto.EventID{eventID})
			if err != nil {
				t.Fatal(err)
			}

			zlog.Info("event deferred", "eventID", eventID, "duration", testDefer)
		}

		byID[eventID] = testData
	}

	started := time.Now()
	iter := 1

	zlog.Info("current time", "now", float64(time.Now().UnixMicro())/1_000_000) // убедиться, что работает для разных вариантов округления

	fetchAndCompare(ctx, t, &iter, client, byID)

	if len(byID) == 0 {
		t.Fatal("all items received in one batch, looks like Defer is broken")
	}

	for {
		time.Sleep(testPollDelay)
		fetchAndCompare(ctx, t, &iter, client, byID)

		if len(byID) == 0 {
			return
		}

		elapsed := time.Since(started)

		if elapsed.Truncate(testPollDelay) > testDefer+2*testPollDelay {
			keys := make([]queuedproto.EventID, 0, len(byID))
			for key := range byID {
				keys = append(keys, key)
			}

			advslog.FatalContext(ctx, "haven't received all deferred items", "event_ids", keys, "elapsed", elapsed, "iterations", iter)
		}
	}
}

func fetchAndCompare(ctx context.Context, t *testing.T, iter *int, client *queuedclient.QueuedClient, byID eventDataMap) {
	events, err := client.GetActive(ctx, testRawQueueID, 2*testBatchSize) // двойной размер - чтобы получить все задержанные значения от предыдущих (сфейливших/прерванных) запусков теста )
	if err != nil {
		t.Fatal(err)
	}

	for _, event := range events.Items {
		if err = client.Delete(ctx, testRawQueueID, []queuedproto.EventID{event.EventID}); err != nil {
			t.Fatal(err)
		}

		zlog := advslog.Ctx(ctx)
		expected, ok := byID[event.EventID]

		if !ok {
			zlog.Warn("unexpected event received", "iter", *iter, "eventID", event.EventID)
			continue
		}

		if !bytes.Equal(event.Data, expected) {
			t.Fatalf("iter=%d, eventID=%v: got %v, expected %v", *iter, event.EventID, event.Data, expected)
		}

		zlog.Info("event received", "iter", *iter, "eventID", event.EventID)
		delete(byID, event.EventID)
	}

	*iter++
}

func TestSharding(t *testing.T) {
	ctx, client := getTestClient(t)
	zlog := advslog.Ctx(ctx)

	shardNum := client.ShardNum()
	shardNums := make([]uint, shardNum)
	eventIDs := make([]queuedproto.EventID, shardNum)
	plainEventIDMap := make(map[uint64]uint, shardNum)
	plainEventIDs := make([]uint64, 0, shardNum)

	zlog.Info("got shards", "count", shardNum)

	for i := uint(0); i < shardNum; i++ {
		evID, err := client.Publish(ctx, testShardingQueueID, queuedproto.EventID{
			Shard: queuedclient.BalanceShard,
			ID:    uint64(i + 1),
		}, queuedproto.FlagIgnoreWrongItem, []byte{})
		if err != nil {
			advslog.FatalContext(ctx, "Publish failed", advslog.Err(err))
		}

		shardNums[i] = evID.Shard
		eventIDs[i] = evID
		plainEventIDMap[evID.ID] = evID.Shard

		zlog.Info("event published", "event_id", evID)
	}

	// проверяем, что все номера шардов отличаются - сортируем и попарно сравниваем.
	// номера шардов действительно могут совпадать, если часть шардов лежит - т.е. легла сразу после установки соединения,
	// что в условиях этого теста крайне маловероятно.
	sort.Slice(shardNums, func(i, j int) bool {
		return shardNums[i] < shardNums[j]
	})

	for i, shard := range shardNums[1:] {
		if shard == shardNums[i] {
			advslog.FatalContext(ctx, "duplicate shard numbers", "shard_nums", shardNums)
		}
	}

	for id := range plainEventIDMap {
		plainEventIDs = append(plainEventIDs, id)
	}

	items, err := client.GetByIDsBroadcast(ctx, testShardingQueueID, plainEventIDs)
	if err != nil {
		advslog.FatalContext(ctx, "GetByIDsBroadcast", advslog.Err(err), "event_ids", plainEventIDs)
	}

	if len(items.Items) != int(shardNum) {
		advslog.FatalContext(ctx, "unexpected item count", "got", len(items.Items), "expected", shardNum)
	}

	for _, item := range items.Items {
		shard, ok := plainEventIDMap[item.EventID.ID]
		if !ok {
			t.Errorf("got unexpected item %v", item.EventID)
		}

		if item.EventID.Shard != shard {
			t.Fatalf("item %v: got shard %d, expected %d", item.EventID, item.EventID.Shard, shard)
		}
	}

	if err := client.Delete(ctx, testShardingQueueID, eventIDs); err != nil {
		advslog.FatalContext(ctx, "Delete failed", advslog.Err(err), "event_ids", eventIDs)
	}
}

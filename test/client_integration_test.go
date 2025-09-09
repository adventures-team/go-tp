//go:build integration
// +build integration

package tptest

import (
	"bytes"
	"context"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/my-mail-ru/go-queuedproto"

	"gitlab.cebc.tools/go/tp.git/queuedclient"
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
	zLog := zerolog.Ctx(ctx)
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

		zLog.Info().Stringer("eventID", eventID).Msg("event published")

		if rand.Intn(2) == 1 {
			err = client.Defer(ctx, testRawQueueID, testDefer, []queuedproto.EventID{eventID})
			if err != nil {
				t.Fatal(err)
			}

			zLog.Info().Stringer("eventID", eventID).Stringer("duration", testDefer).Msg("event deferred")
		}

		byID[eventID] = testData
	}

	started := time.Now()
	iter := 1

	zLog.Info().Float64("now", float64(time.Now().UnixMicro())/1_000_000).Send() // убедиться, что работает для разных вариантов округления

	getItems(ctx, t, &iter, client, byID)

	if len(byID) == 0 {
		t.Fatal("all items received in one batch, looks like Defer is broken")
	}

	for {
		time.Sleep(testPollDelay)
		getItems(ctx, t, &iter, client, byID)

		if len(byID) == 0 {
			return
		}

		elapsed := time.Since(started)

		if elapsed.Truncate(testPollDelay) > testDefer+2*testPollDelay {
			keys := make([]queuedproto.EventID, 0, len(byID))
			for key := range byID {
				keys = append(keys, key)
			}

			zLog.Fatal().Msgf("haven't received all deferred items %v in %v, %d iterations", keys, elapsed, iter)
		}
	}
}

func getItems(ctx context.Context, t *testing.T, iter *int, client *queuedclient.QueuedClient, byID eventDataMap) {
	events, err := client.GetActive(ctx, testRawQueueID, 2*testBatchSize) // двойной размер - чтобы получить все задержанные значения от предыдущих (сфейливших/прерванных) запусков теста )
	if err != nil {
		t.Fatal(err)
	}

	for _, event := range events.Items {
		if err = client.Delete(ctx, testRawQueueID, []queuedproto.EventID{event.EventID}); err != nil {
			t.Fatal(err)
		}

		zLog := zerolog.Ctx(ctx)
		expected, ok := byID[event.EventID]

		if !ok {
			zLog.Warn().Int("iter", *iter).Stringer("eventID", event.EventID).Msg("unexpected event received")
			continue
		}

		if !bytes.Equal(event.Data, expected) {
			t.Fatalf("iter=%d, eventID=%v: got %v, expected %v", *iter, event.EventID, event.Data, expected)
		}

		zLog.Info().Int("iter", *iter).Stringer("eventID", event.EventID).Msg("event received")
		delete(byID, event.EventID)
	}

	*iter++
}

func TestSharding(t *testing.T) {
	ctx, client := getTestClient(t)
	zLog := zerolog.Ctx(ctx)

	shardNum := client.ShardNum()
	shardNums := make([]uint, shardNum)
	eventIDs := make([]queuedproto.EventID, shardNum)
	plainEventIDMap := make(map[uint64]uint, shardNum)
	plainEventIDs := make([]uint64, 0, shardNum)

	zLog.Info().Msgf("got %d shards", shardNum)

	for i := uint(0); i < shardNum; i++ {
		evID, err := client.Publish(ctx, testShardingQueueID, queuedproto.EventID{
			Shard: queuedclient.BalanceShard,
			ID:    uint64(i + 1),
		}, queuedproto.FlagIgnoreWrongItem, []byte{})
		if err != nil {
			zLog.Fatal().Err(err).Send()
		}

		shardNums[i] = evID.Shard
		eventIDs[i] = evID
		plainEventIDMap[evID.ID] = evID.Shard

		zLog.Info().Stringer("event_id", evID).Msg("event published")
	}

	// проверяем, что все номера шардов отличаются - сортируем и попарно сравниваем.
	// номера шардов действительно могут совпадать, если часть шардов лежит - т.е. легла сразу после установки соединения,
	// что в условиях этого теста крайне маловероятно.
	sort.Slice(shardNums, func(i, j int) bool {
		return shardNums[i] < shardNums[j]
	})

	for i, shard := range shardNums[1:] {
		if shard == shardNums[i] {
			zLog.Fatal().Uints("shard_nums", shardNums).Msg("duplicate shard numbers")
		}
	}

	for id := range plainEventIDMap {
		plainEventIDs = append(plainEventIDs, id)
	}

	items, err := client.GetByIDsBroadcast(ctx, testShardingQueueID, plainEventIDs)
	if err != nil {
		zLog.Fatal().Err(err).Uints64("event_ids", plainEventIDs).Msg("GetByIDsBroadcast")
	}

	if len(items.Items) != int(shardNum) {
		zLog.Fatal().Msgf("got %d items, expected %d (shards count)", len(items.Items), shardNum)
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
		zLog.Fatal().Err(err).Interface("event_ids", eventIDs).Msg("Delete failed")
	}
}

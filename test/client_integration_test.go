//go:build integration

package tptest

import (
	"bytes"
	"context"
	"errors"
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

// TestQueueStat - статистика очереди по шардам (ReqQueueStat версии queuedclient.QueueStatVersion
// на реальном queued): после публикации события учтены в ItemsCount, выданные GetActive -
// в LockedCount, несуществующая очередь даёт нули.
func TestQueueStat(t *testing.T) {
	ctx, client := getTestClient(t)
	zlog := advslog.Ctx(ctx)

	// очередь могла остаться непустой после сфейлившего запуска - выгребаем всё
	drainQueue(ctx, t, client, testStatQueueID)

	stats, err := client.QueueStat(ctx, testUnusedQueueID)
	if err != nil {
		t.Fatalf("QueueStat(unused queue): %v", err)
	}

	if len(stats) != int(client.ShardNum()) {
		t.Fatalf("QueueStat(unused queue): %d shards in result, want %d", len(stats), client.ShardNum())
	}

	for shard, stat := range stats {
		if stat != (queuedproto.RespQueueStat{}) {
			t.Errorf("QueueStat(unused queue): shard %d: %+v, want zero stat", shard, stat)
		}
	}

	eventIDs := make([]queuedproto.EventID, 0, testBatchSize)

	for i := 0; i < testBatchSize; i++ {
		eventID, err := client.Publish(ctx, testStatQueueID, queuedproto.EventID{Shard: queuedclient.BalanceShard}, 0, []byte("stat"))
		if err != nil {
			t.Fatal(err)
		}

		eventIDs = append(eventIDs, eventID)
	}

	defer func() {
		if err := client.Delete(ctx, testStatQueueID, eventIDs); err != nil {
			t.Errorf("cleanup Delete: %v", err)
		}
	}()

	items, active, locked := sumQueueStat(ctx, t, client, testStatQueueID)
	zlog.Info("after publish", "items", items, "active", active, "locked", locked)

	if items != testBatchSize || locked != 0 {
		t.Fatalf("after publish: items %d, locked %d; want items %d, locked 0", items, locked, testBatchSize)
	}

	// queued активирует новые события периодической процедурой, а не в момент публикации -
	// ждём, пока GetActive выдаст (и заблокирует) всю пачку
	fetched := 0
	deadline := time.Now().Add(10 * time.Second)

	for fetched < testBatchSize {
		if time.Now().After(deadline) {
			t.Fatalf("GetActive: fetched %d of %d events before the deadline", fetched, testBatchSize)
		}

		events, err := client.GetActive(ctx, testStatQueueID, testBatchSize)
		if err != nil {
			t.Fatal(err)
		}

		fetched += len(events.Items)

		if len(events.Items) == 0 {
			time.Sleep(testPollDelay / 5)
		}
	}

	items, active, locked = sumQueueStat(ctx, t, client, testStatQueueID)
	zlog.Info("after GetActive", "fetched", fetched, "items", items, "active", active, "locked", locked)

	if locked != testBatchSize {
		t.Errorf("after GetActive: locked %d, want %d (fetched items)", locked, testBatchSize)
	}

	if items != testBatchSize {
		t.Errorf("after GetActive: items %d, want %d (locked items are still counted)", items, testBatchSize)
	}
}

// sumQueueStat суммирует статистику очереди по шардам.
func sumQueueStat(ctx context.Context, t *testing.T, client *queuedclient.QueuedClient, queueID uint16) (items, active, locked uint32) {
	t.Helper()

	stats, err := client.QueueStat(ctx, queueID)
	if err != nil {
		t.Fatalf("QueueStat(%d): %v", queueID, err)
	}

	for _, stat := range stats {
		items += stat.ItemsCount
		active += stat.ActiveCount
		locked += stat.LockedCount
	}

	return items, active, locked
}

// drainQueue удаляет из очереди все активные события (остатки предыдущих запусков тестов).
func drainQueue(ctx context.Context, t *testing.T, client *queuedclient.QueuedClient, queueID uint16) {
	t.Helper()

	for {
		events, err := client.GetActive(ctx, queueID, 2*testBatchSize)
		if err != nil && len(events.Items) == 0 {
			var errMulti queuedclient.ErrMulti

			if errors.As(err, &errMulti) && len(dropUnknownQueue(errMulti)) == 0 {
				return // очереди ещё нет ни в одном шарде
			}

			t.Fatalf("drainQueue(%d): %v", queueID, err)
		}

		if len(events.Items) == 0 {
			return
		}

		ids := make([]queuedproto.EventID, 0, len(events.Items))
		for _, event := range events.Items {
			ids = append(ids, event.EventID)
		}

		if err := client.Delete(ctx, queueID, ids); err != nil {
			t.Fatalf("drainQueue(%d): Delete: %v", queueID, err)
		}
	}
}

// dropUnknownQueue возвращает ошибки шардов, кроме "unknown queue type".
func dropUnknownQueue(errs queuedclient.ErrMulti) queuedclient.ErrMulti {
	rest := make(queuedclient.ErrMulti, 0, len(errs))

	for _, err := range errs {
		if !errors.Is(err, queuedproto.ErrUnknownQueueType) {
			rest = append(rest, err)
		}
	}

	return rest
}

//go:build integration
// +build integration

package tptest

import (
	"bytes"
	"context"
	"math/rand"
	"testing"
	"time"

	"gitlab.corp.mail.ru/mygo/queuedproto.git"
	"gitlab.corp.mail.ru/mygo/tp.git/queuedclient"
)

// Тест только для queuedclient, безо всей высокоуровневой логики.
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

	if bytes.Compare(events.Items[0].Data, testData) != 0 {
		t.Fatalf("testData: got: %s, expected: %s", events.Items[0].Data, testData)
	}

	if err = client.Delete(ctx, testRawQueueID, []queuedproto.EventID{eventID}); err != nil {
		t.Fatal(err)
	}
}

func TestBatch(t *testing.T) {
	ctx, client := getTestClient(t)

	byID := eventDataMap{}

	for i := 0; i < testBatchSize; i++ {
		testData := make([]byte, 10)
		_, err := rand.Read(testData)
		if err != nil {
			t.Fatalf("rand.Read: %v", err)
		}

		eventID, err := client.Publish(ctx, testRawQueueID, queuedproto.EventID{Shard: queuedclient.BalanceShard, ID: 0}, 0, testData)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("eventID=%v published", eventID)

		if rand.Intn(2) == 1 {
			err = client.Defer(ctx, testRawQueueID, testDefer, []queuedproto.EventID{eventID})
			if err != nil {
				t.Fatal(err)
			}

			t.Logf("eventID=%v deferred for %v", eventID, testDefer)
		}

		byID[eventID] = testData
	}

	started := time.Now()
	iter := 1

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

		if elapsed.Round(testPollDelay) > testDefer {
			keys := make([]queuedproto.EventID, 0, len(byID))
			for key := range byID {
				keys = append(keys, key)
			}

			t.Fatalf("haven't received all deferred items %v in %v", keys, elapsed)
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

		expected, ok := byID[event.EventID]
		if !ok {
			t.Logf("unexpected event received: iter=%v, eventID=%v", *iter, event.EventID)
			continue
		}

		if bytes.Compare(event.Data, expected) != 0 {
			t.Fatalf("iter=%d, eventID=%v: got %v, expected %v", *iter, event.EventID, event.Data, expected)
		}

		t.Logf("iter=%d, eventID=%v received", *iter, event.EventID)
		delete(byID, event.EventID)
	}

	*iter++
}

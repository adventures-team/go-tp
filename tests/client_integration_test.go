//go:build integration
// +build integration

package tptest

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	"gitlab.corp.mail.ru/mygo/tp.git/queuedclient"
)

func TestSingle(t *testing.T) {
	testData := []byte("test event")

	client := getTestClient(t)

	eventID, err := client.Publish(testRawQueueID, 0, 0, testData)
	if err != nil {
		t.Fatal(err)
	}

	events, err := client.GetByIDs(testRawQueueID, []uint64{eventID})
	if err != nil {
		t.Fatalf("GetByIDs(%d): %v", eventID, err)
	}

	if len(events.Items) != 1 {
		t.Fatalf("strange number of items received: %#v", events)
	}

	if bytes.Compare(events.Items[0].Data, testData) != 0 {
		t.Fatalf("testData: got: %s, expected: %s", events.Items[0].Data, testData)
	}

	if err = client.Delete(testRawQueueID, []uint64{eventID}); err != nil {
		t.Fatal(err)
	}
}

func TestBatch(t *testing.T) {
	client := getTestClient(t)

	byID := map[uint64][]byte{}

	for i := 0; i < testBatchSize; i++ {
		testData := make([]byte, 10)
		_, err := rand.Read(testData)
		if err != nil {
			t.Fatalf("rand.Read: %v", err)
		}

		eventID, err := client.Publish(testRawQueueID, 0, 0, testData)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("eventID=%d published", eventID)

		if rand.Intn(2) == 1 {
			err = client.Defer(testRawQueueID, testDefer, []uint64{eventID})
			if err != nil {
				t.Fatal(err)
			}

			t.Logf("eventID=%d deferred for %v", eventID, testDefer)
		}

		byID[eventID] = testData
	}

	started := time.Now()
	iter := 1

	getItems(t, &iter, client, byID)

	if len(byID) == 0 {
		t.Fatal("all items received in one batch, looks like Defer is broken")
	}

	for {
		time.Sleep(testPollDelay)
		getItems(t, &iter, client, byID)

		if len(byID) == 0 {
			return
		}

		elapsed := time.Since(started)

		if elapsed.Round(testPollDelay) > testDefer {
			keys := make([]uint64, 0, len(byID))
			for key := range byID {
				keys = append(keys, key)
			}

			t.Fatalf("haven't received all deferred items %v in %v", keys, elapsed)
		}
	}
}

func getItems(t *testing.T, iter *int, client *queuedclient.QueuedClient, byID map[uint64][]byte) {
	events, err := client.GetActive(testRawQueueID, 2*testBatchSize) // двойной размер - чтобы получить все задержанные значения от предыдущих (сфейливших/прерванных) запусков теста )
	if err != nil {
		t.Fatal(err)
	}

	for _, event := range events.Items {
		if err = client.Delete(testRawQueueID, []uint64{event.ID}); err != nil {
			t.Fatal(err)
		}

		expected, ok := byID[event.ID]
		if !ok {
			t.Logf("unexpected event received: iter=%d, eventID=%d", *iter, event.ID)
			continue
		}

		if bytes.Compare(event.Data, expected) != 0 {
			t.Fatalf("iter=%d, eventID=%d: got %v, expected %v", *iter, event.ID, event.Data, expected)
		}

		t.Logf("iter=%d, eventID=%d received", *iter, event.ID)
		delete(byID, event.ID)
	}

	*iter++
}

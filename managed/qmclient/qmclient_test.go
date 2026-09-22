package qmclient

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adventures-team/go-iprotoconn/config"
	"github.com/go-faker/faker/v4"
	"github.com/google/go-cmp/cmp"
)

func TestHandleStats(t *testing.T) {
	data := StatsData{}
	_ = faker.FakeData(&data)

	client := Client{
		stats: func() StatsData { return data },
	}

	got := client.handleStats(context.Background(), &StatsRequest{})

	want := &StatsResponse{
		Status:  answerOK,
		Data:    data,
		Cluster: cluster,
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("handleStats mismatch (-want +got):\n%s", diff)
	}
}

func TestHandleSetQueue(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		assignSlotCalls := 0
		data := StatsData{}

		client := Client{
			stats: func() StatsData { return data },
			assignSlot: func(uint32, uint32) error {
				assignSlotCalls++
				return nil
			},
		}

		got := client.handleSetQueue(context.Background(), &SetQueueRequest{
			Queues: []SetQueue{{Slot: 0, QueueID: 1}, {Slot: 1, QueueID: 2}},
		})

		want := &SetQueueResponse{
			Status:           answerOK,
			SetQueueStatuses: []uint32{answerOK, answerOK},
			Stats: StatsResponse{
				Status:  answerOK,
				Data:    data,
				Cluster: cluster,
			},
		}

		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("handleSetQueue mismatch (-want +got):\n%s", diff)
		}

		if want := 2; assignSlotCalls != want {
			t.Errorf("unexpected number of assignSlot calls: got %d, want %d", assignSlotCalls, want)
		}
	})

	t.Run("assign error", func(t *testing.T) {
		client := Client{
			stats: func() StatsData { return StatsData{} },
			assignSlot: func(_, queueID uint32) error {
				if queueID == 2 {
					return errors.New("slot is busy")
				}

				return nil
			},
		}

		got := client.handleSetQueue(context.Background(), &SetQueueRequest{
			Queues: []SetQueue{{Slot: 0, QueueID: 1}, {Slot: 1, QueueID: 2}},
		})

		if want := []uint32{answerOK, answerFail}; !cmp.Equal(want, got.SetQueueStatuses) {
			t.Errorf("SetQueueStatuses: got %v, want %v", got.SetQueueStatuses, want)
		}
	})
}

// TestRequestCodecs - страховка соответствия типов запросов контрактам
// go-iprotoconn (Cmd + Unmarshal): регистрация в мультиплексоре проверяет
// это на этапе компиляции, тест фиксирует коды команд протокола.
func TestRequestCodecs(t *testing.T) {
	if got := (StatsRequest{}).Cmd(); got != cmdStat {
		t.Errorf("StatsRequest.Cmd() = %d, want %d", got, cmdStat)
	}

	if got := (SetQueueRequest{}).Cmd(); got != cmdSetQueue {
		t.Errorf("SetQueueRequest.Cmd() = %d, want %d", got, cmdSetQueue)
	}

	req := SetQueueRequest{Queues: []SetQueue{{Slot: 3, QueueID: 7, Flags: 1}}}

	data, err := req.MarshalIProto(nil)
	if err != nil {
		t.Fatal(err)
	}

	var decoded SetQueueRequest

	rest, err := decoded.UnmarshalIProto(data)
	if err != nil {
		t.Fatal(err)
	}

	if len(rest) != 0 {
		t.Fatalf("unexpected tail: %d bytes", len(rest))
	}

	if diff := cmp.Diff(req, decoded); diff != "" {
		t.Errorf("SetQueueRequest roundtrip mismatch (-want +got):\n%s", diff)
	}
}

// TestQMSourceSubscribable: адрес менеджера фиксирован, но источник должен поддерживать
// интерфейс подписок - иначе go-iprotoconn при каждом запуске предупреждает об отключённом
// горячем переподключении. Подписка принимается и никогда не срабатывает.
func TestQMSourceSubscribable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updates := config.NewTopologyProvider(qmSource{endpoint: "127.0.0.1:7877"}).Watch(ctx)
	if updates == nil {
		t.Fatal("Watch returned nil: qmSource is not seen as config.Subscribable")
	}

	select {
	case top := <-updates:
		t.Fatalf("unexpected topology update: %+v", top)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()

	select {
	case _, ok := <-updates:
		if ok {
			t.Fatal("unexpected topology update after cancel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("updates channel is not closed after cancel")
	}
}

package managedmaster

import (
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/mock/gomock"

	"github.com/adventures-team/go-tp"
	"github.com/adventures-team/go-tp/managed/qmclient"
	"github.com/adventures-team/go-tp/managed/stats"
	"github.com/adventures-team/go-tp/slotpool"
	mockpoller "github.com/adventures-team/go-tp/test/mock/poller"
	mockqueue "github.com/adventures-team/go-tp/test/mock/queue"
	mockslotpool "github.com/adventures-team/go-tp/test/mock/slotpool"
	mockstat "github.com/adventures-team/go-tp/test/mock/stats"
)

func TestAssignSlot(t *testing.T) {
	t.Run("assign work to slot", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		q := mockqueue.NewMockQueue(ctrl)
		p := mockslotpool.NewMockPool(ctrl)
		slotID := uint32(0)
		queueID := uint32(10)

		slotStats := make(chan map[uint32]SlotStat, 1)
		slotStats <- make(map[uint32]SlotStat)

		mm := ManagedMaster{
			queue:     q,
			pool:      p,
			poller:    nil,
			logger:    slog.New(slog.DiscardHandler),
			slotStats: slotStats,
		}

		q.EXPECT().IsRegistered(uint16(queueID)).Return(true)
		p.EXPECT().AssignSlot(slotID, gomock.Any()).DoAndReturn(func(_ uint32, w slotpool.Work) error {
			if w.ID != queueID {
				t.Error("unexpected queueID")
			}

			return nil
		})

		err := mm.AssignSlot(slotID, uint32(queueID))
		if err != nil {
			t.Errorf("AssignSlot(%d, %d) = %v, no error expected", slotID, queueID, err)
		}
	})

	t.Run("release slot", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		q := mockqueue.NewMockQueue(ctrl)
		p := mockslotpool.NewMockPool(ctrl)
		slotID := uint32(0)
		queueID := uint32(0)

		slotStats := make(chan map[uint32]SlotStat, 1)
		slotStats <- make(map[uint32]SlotStat)

		mm := ManagedMaster{
			queue:     q,
			pool:      p,
			poller:    nil,
			logger:    slog.New(slog.DiscardHandler),
			slotStats: slotStats,
		}

		p.EXPECT().ReleaseSlot(slotID).Return(nil)

		err := mm.AssignSlot(slotID, queueID)
		if err != nil {
			t.Errorf("AssignSlot(%d, %d) = %v, no error expected", slotID, queueID, err)
		}
	})

	t.Run("error on release slot", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		q := mockqueue.NewMockQueue(ctrl)
		p := mockslotpool.NewMockPool(ctrl)
		slotID := uint32(0)
		queueID := uint32(0)

		slotStats := make(chan map[uint32]SlotStat, 1)
		stats := make(map[uint32]SlotStat)

		slotStats <- stats

		mm := ManagedMaster{
			queue:     q,
			pool:      p,
			poller:    nil,
			logger:    slog.New(slog.DiscardHandler),
			slotStats: slotStats,
		}

		wantErr := errors.New("Oops")
		p.EXPECT().ReleaseSlot(slotID).Return(wantErr)

		gotErr := mm.AssignSlot(slotID, queueID)
		if gotErr != wantErr {
			t.Errorf("AssignSlot(%d, %d) = %v, error expected", slotID, queueID, gotErr)
		}

		stat := stats[0].Stat()
		gotSlotErrors := stat.SlotErrors
		wantSlotErrors := 1

		if gotSlotErrors != wantSlotErrors {
			t.Errorf("unexpected number of slot errors: got %d, want %d", gotSlotErrors, wantSlotErrors)
		}

		if stat.QueueID != 0 {
			t.Error("QueueID should be cleared")
		}
	})

	t.Run("queue is not registered", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		q := mockqueue.NewMockQueue(ctrl)
		p := mockslotpool.NewMockPool(ctrl)
		slotID := uint32(0)
		queueID := uint32(10)

		slotStats := make(chan map[uint32]SlotStat, 1)
		slotStats <- make(map[uint32]SlotStat)

		mm := ManagedMaster{
			queue:     q,
			pool:      p,
			poller:    nil,
			logger:    slog.New(slog.DiscardHandler),
			slotStats: slotStats,
		}

		q.EXPECT().IsRegistered(uint16(queueID)).Return(false)

		err := mm.AssignSlot(slotID, uint32(queueID))

		got, want := err, tp.ErrUnknownQueue
		if got != want {
			t.Errorf("unexpected error: got %v, want %v", got, want)
		}
	})

	t.Run("slot not found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		q := mockqueue.NewMockQueue(ctrl)
		p := mockslotpool.NewMockPool(ctrl)
		slotID := uint32(0)
		queueID := uint32(10)

		slotStats := make(chan map[uint32]SlotStat, 1)
		slotStats <- make(map[uint32]SlotStat)

		mm := ManagedMaster{
			queue:     q,
			pool:      p,
			poller:    nil,
			logger:    slog.New(slog.DiscardHandler),
			slotStats: slotStats,
		}

		q.EXPECT().IsRegistered(uint16(queueID)).Return(true)
		p.EXPECT().AssignSlot(slotID, gomock.Any()).Return(slotpool.ErrSlotNotFound)

		err := mm.AssignSlot(slotID, uint32(queueID))

		got, want := err, slotpool.ErrSlotNotFound
		if got != want {
			t.Errorf("unexpected error: got %v, want %v", got, want)
		}
	})
}

func TestWork(t *testing.T) {
	t.Run("execute work", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		q := mockqueue.NewMockQueue(ctrl)
		pool := mockslotpool.NewMockPool(ctrl)
		poller := mockpoller.NewMockPoller(ctrl)
		worker := mockpoller.NewMockWorker(ctrl)
		workerStat := mockpoller.NewMockWorkerStat(ctrl)
		queueID := uint32(10)
		slotID := uint32(0)

		pool.EXPECT().SlotStates().Return(nil)

		mm := NewManagedMaster(q, pool, poller, slog.New(slog.DiscardHandler))

		poller.EXPECT().Poll(uint16(queueID)).Return(worker)
		worker.EXPECT().Work()
		workerStat.EXPECT().Processed().Return(1)
		workerStat.EXPECT().Errors().Return(0)
		worker.EXPECT().Stat().Return(workerStat).Times(2)

		mm.work(slotID, queueID)
	})

	t.Run("no events in queue", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		q := mockqueue.NewMockQueue(ctrl)
		pool := mockslotpool.NewMockPool(ctrl)
		poller := mockpoller.NewMockPoller(ctrl)
		queueID := uint32(10)
		slotID := uint32(0)

		pool.EXPECT().SlotStates().Return(nil)
		mm := NewManagedMaster(q, pool, poller, slog.New(slog.DiscardHandler))

		poller.EXPECT().Poll(uint16(queueID)).Return(nil)

		mm.work(slotID, queueID)
	})
}

func TestDelay(t *testing.T) {
	ctrl := gomock.NewController(t)
	pool := mockslotpool.NewMockPool(ctrl)
	queueID := uint32(100)

	pool.EXPECT().SlotStates().Return(nil)
	mm := NewManagedMaster(nil, pool, nil, nil)

	want := []float64{0.1, 0.272, 0.73984, 2.0123648000000003, 5.473632256000001, 10, 10}

	got := make([]float64, 0, 7)
	for range want {
		got = append(got, mm.delay(queueID))
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("delay(%d) returned unexpected difference (-want +got):\n%s", queueID, diff)
	}
}

func TestResetDelay(t *testing.T) {
	queueID := uint32(100)

	delays := make(chan map[uint32]float64, 1)
	delays <- map[uint32]float64{queueID: 100}

	mm := ManagedMaster{delays: delays}

	mm.resetDelay(queueID)

	d := <-delays

	got, want := d[queueID], float64(0)
	if got != want {
		t.Errorf("delay was not reset: got %f, want %f", got, want)
	}
}

func BenchmarkDelay(b *testing.B) {
	queueID := uint32(100)

	delays := make(chan map[uint32]float64, 1)
	delays <- make(map[uint32]float64)

	mm := &ManagedMaster{
		delays: delays,
	}

	b.ResetTimer()

	for range b.N {
		mm.delay(queueID)
	}
}

func BenchmarkResetDelay(b *testing.B) {
	queueID := uint32(100)

	delays := make(chan map[uint32]float64, 1)
	delays <- make(map[uint32]float64)

	mm := &ManagedMaster{
		delays: delays,
	}

	b.ResetTimer()

	for range b.N {
		mm.resetDelay(queueID)
	}
}

func TestStats(t *testing.T) {
	t.Run("no slots", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		pool := mockslotpool.NewMockPool(ctrl)

		pool.EXPECT().SlotStates().MaxTimes(2).Return(nil)
		mm := NewManagedMaster(nil, pool, nil, slog.New(slog.DiscardHandler))

		now := uint32(time.Now().Unix())
		want := qmclient.StatsData{
			PID:                   uint32(os.Getpid()),
			StartedAt:             now,
			LastSlotStatUpdatedAt: now,
			TotalSlots:            0,
			UnknownSlots:          0,
			AdminConsolePort:      sentinel,
			Slots:                 []qmclient.SlotStat{},
		}

		got := mm.Stats()

		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("Stats() returned unexpected difference (-want +got):\n%s", diff)
		}
	})

	t.Run("one inactive slot", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		pool := mockslotpool.NewMockPool(ctrl)
		slot := slotpool.SlotState{
			StartedAt:     uint32(time.Now().Unix()),
			UpdatedAt:     0,
			WorkID:        0,
			PendingWorkID: 0,
		}

		pool.EXPECT().SlotStates().MaxTimes(2).Return([]slotpool.SlotState{slot})
		mm := NewManagedMaster(nil, pool, nil, slog.New(slog.DiscardHandler))

		now := uint32(time.Now().Unix())
		want := qmclient.StatsData{
			PID:                   uint32(os.Getpid()),
			StartedAt:             now,
			LastSlotStatUpdatedAt: now,
			TotalSlots:            1,
			UnknownSlots:          0,
			AdminConsolePort:      sentinel,
			Slots: []qmclient.SlotStat{{
				StartTime:      slot.StartedAt,
				Status:         uint32(stats.Idle),
				PID:            uint32(os.Getpid()),
				SlotNumber:     0,
				State:          uint32(stats.Idle),
				ArchivedQueues: []qmclient.QueueExtendedStat{},
			}},
		}

		got := mm.Stats()

		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("Stats() returned unexpected difference (-want +got):\n%s", diff)
		}
	})

	t.Run("one active slot", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		pool := mockslotpool.NewMockPool(ctrl)
		slotStat := mockstat.NewMockSlotStat(ctrl)

		now := uint32(time.Now().Unix())
		queueID := uint32(10)
		pid := uint32(os.Getpid())
		state := uint32(stats.Working)

		slot := slotpool.SlotState{
			StartedAt:     now,
			UpdatedAt:     now,
			WorkID:        queueID,
			PendingWorkID: 0,
		}
		queueStat := stats.QueueStat{
			ID:        queueID,
			StartedAt: now,
			Current: stats.QueueCounters{
				Processed: 10,
				Loops:     5,
				WorkTime:  4.1,
				WaitTime:  7.1,
				Started:   1,
				Stopped:   0,
			},
		}

		pool.EXPECT().SlotStates().MaxTimes(2).Return([]slotpool.SlotState{slot})

		slotStats := make(chan map[uint32]SlotStat, 1)
		slotStats <- map[uint32]SlotStat{0: slotStat}

		mm := ManagedMaster{
			pool:      pool,
			logger:    slog.New(slog.DiscardHandler),
			startedAt: uint32(time.Now().Unix()),
			pid:       uint32(os.Getpid()),
			slotStats: slotStats,
		}

		want := qmclient.StatsData{
			PID:                   pid,
			StartedAt:             now,
			LastSlotStatUpdatedAt: now,
			TotalSlots:            1,
			UnknownSlots:          0,
			AdminConsolePort:      sentinel,
			Slots: []qmclient.SlotStat{{
				UpdateTime: slot.UpdatedAt,
				StartTime:  slot.StartedAt,
				Status:     state,
				PID:        pid,
				SlotNumber: 0,
				State:      state,
				QueueStat: qmclient.QueueExtendedStat{
					QueueID:   queueStat.ID,
					StartedAt: queueStat.StartedAt,
					Current: qmclient.QueueStat{
						EvalErrors: 0,
						Processed:  queueStat.Current.Processed,
						Loops:      queueStat.Current.Loops,
						WorkTime:   queueStat.Current.WorkTime,
						WaitTime:   queueStat.Current.WaitTime,
						Started:    queueStat.Current.Started,
					},
					Total: qmclient.QueueStat{},
				},
				ArchivedQueues: []qmclient.QueueExtendedStat{},
			}},
		}

		slotStat.EXPECT().Stat().Return(stats.SlotStat{
			ID:         0,
			PID:        pid,
			StartedAt:  0,
			State:      stats.State(state),
			QueueID:    queueID,
			Switches:   0,
			SlotErrors: 0,
			Queues:     []stats.QueueStat{queueStat},
		})

		got := mm.Stats()

		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("Stats() returned unexpected difference (-want +got):\n%s", diff)
		}
	})

	t.Run("switching state", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		pool := mockslotpool.NewMockPool(ctrl)
		slotStat := mockstat.NewMockSlotStat(ctrl)

		now := uint32(time.Now().Unix())
		queueID := uint32(10)
		pid := uint32(os.Getpid())
		state := stats.Working
		slot := slotpool.SlotState{
			StartedAt:     uint32(time.Now().Unix()),
			WorkID:        queueID,
			PendingWorkID: 2,
		}

		queueStat := stats.QueueStat{
			ID:        queueID,
			StartedAt: now,
			Current: stats.QueueCounters{
				Processed: 10,
				Loops:     5,
				WorkTime:  4.1,
				WaitTime:  7.1,
				Started:   1,
				Stopped:   0,
			},
		}

		pool.EXPECT().SlotStates().MaxTimes(2).Return([]slotpool.SlotState{slot})

		slotStats := make(chan map[uint32]SlotStat, 1)
		slotStats <- map[uint32]SlotStat{0: slotStat}

		mm := ManagedMaster{
			pool:      pool,
			logger:    slog.New(slog.DiscardHandler),
			startedAt: now,
			pid:       pid,
			slotStats: slotStats,
		}

		want := qmclient.StatsData{
			PID:                   uint32(os.Getpid()),
			StartedAt:             now,
			LastSlotStatUpdatedAt: now,
			TotalSlots:            1,
			UnknownSlots:          0,
			AdminConsolePort:      sentinel,
			Slots: []qmclient.SlotStat{{
				StartTime:  slot.StartedAt,
				Status:     uint32(state),
				PID:        pid,
				SlotNumber: 0,
				State:      uint32(stats.Switching),
				QueueStat: qmclient.QueueExtendedStat{
					QueueID:   queueID,
					StartedAt: queueStat.StartedAt,
					Current: qmclient.QueueStat{
						EvalErrors: 0,
						Processed:  queueStat.Current.Processed,
						Loops:      queueStat.Current.Loops,
						WorkTime:   queueStat.Current.WorkTime,
						WaitTime:   queueStat.Current.WaitTime,
						Started:    queueStat.Current.Started,
					},
					Total: qmclient.QueueStat{},
				},
				NextQueueID:    slot.PendingWorkID,
				ArchivedQueues: []qmclient.QueueExtendedStat{},
			}},
		}

		slotStat.EXPECT().Stat().Return(stats.SlotStat{
			ID:         0,
			PID:        pid,
			StartedAt:  0,
			State:      stats.State(state),
			QueueID:    queueID,
			Switches:   0,
			SlotErrors: 0,
			Queues:     []stats.QueueStat{queueStat},
		})

		got := mm.Stats()

		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("Stats() returned unexpected difference (-want +got):\n%s", diff)
		}
	})

	t.Run("stat send interval", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		pool := mockslotpool.NewMockPool(ctrl)
		now := uint32(time.Now().Unix())
		slot := slotpool.SlotState{
			StartedAt: now,
		}

		pool.EXPECT().SlotStates().MaxTimes(2).Return([]slotpool.SlotState{slot})
		mm := NewManagedMaster(nil, pool, nil, slog.New(slog.DiscardHandler))
		mm.statSentAt = now - 1000

		stat := mm.Stats()

		got, want := stat.Slots[0].Interval, 1000.0
		if got != want {
			t.Errorf("Stats() returned unexpected intarval value: got %f, want %f", got, want)
		}
	})

	t.Run("queues goes to archive except current one", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		pool := mockslotpool.NewMockPool(ctrl)
		slotStat := mockstat.NewMockSlotStat(ctrl)

		now := uint32(time.Now().Unix())
		currentQueueID := uint32(10)
		archivedQueueID := currentQueueID + 1
		pid := uint32(os.Getpid())
		state := stats.Working
		slot := slotpool.SlotState{
			StartedAt:     uint32(time.Now().Unix()),
			WorkID:        currentQueueID,
			PendingWorkID: 0,
		}

		queueStats := []stats.QueueStat{{
			ID:        currentQueueID,
			StartedAt: now,
			Current: stats.QueueCounters{
				Processed: 10,
				Loops:     5,
				WorkTime:  4.1,
				WaitTime:  7.1,
				Started:   1,
				Stopped:   0,
			},
		}, {
			ID:        archivedQueueID,
			StartedAt: now - 1000,
			StoppedAt: now,
			Current: stats.QueueCounters{
				Processed: 11,
				Loops:     4,
				WorkTime:  10.7,
				WaitTime:  30.3,
				Started:   1,
				Stopped:   1,
			},
		}}

		pool.EXPECT().SlotStates().MaxTimes(2).Return([]slotpool.SlotState{slot})

		slotStats := make(chan map[uint32]SlotStat, 1)
		slotStats <- map[uint32]SlotStat{0: slotStat}

		mm := ManagedMaster{
			pool:      pool,
			logger:    slog.New(slog.DiscardHandler),
			startedAt: now,
			pid:       pid,
			slotStats: slotStats,
		}

		want := qmclient.StatsData{
			PID:                   uint32(os.Getpid()),
			StartedAt:             now,
			LastSlotStatUpdatedAt: now,
			TotalSlots:            1,
			UnknownSlots:          0,
			AdminConsolePort:      sentinel,
			Slots: []qmclient.SlotStat{{
				StartTime:  slot.StartedAt,
				Status:     uint32(state),
				PID:        pid,
				SlotNumber: 0,
				State:      uint32(stats.Working),
				QueueStat: qmclient.QueueExtendedStat{
					QueueID:   currentQueueID,
					StartedAt: queueStats[0].StartedAt,
					Current: qmclient.QueueStat{
						EvalErrors: 0,
						Processed:  queueStats[0].Current.Processed,
						Loops:      queueStats[0].Current.Loops,
						WorkTime:   queueStats[0].Current.WorkTime,
						WaitTime:   queueStats[0].Current.WaitTime,
						Started:    queueStats[0].Current.Started,
					},
					Total: qmclient.QueueStat{},
				},
				NextQueueID: slot.PendingWorkID,
				ArchivedQueues: []qmclient.QueueExtendedStat{{
					QueueID:   archivedQueueID,
					Flags:     0,
					StartedAt: queueStats[1].StartedAt,
					StoppedAt: queueStats[1].StoppedAt,
					Current: qmclient.QueueStat{
						EvalErrors: 0,
						Processed:  queueStats[1].Current.Processed,
						Loops:      queueStats[1].Current.Loops,
						WorkTime:   queueStats[1].Current.WorkTime,
						WaitTime:   queueStats[1].Current.WaitTime,
						Started:    queueStats[1].Current.Started,
						Stopped:    1,
					},
					Total: qmclient.QueueStat{},
				}},
			}},
		}

		slotStat.EXPECT().Stat().Return(stats.SlotStat{
			ID:         0,
			PID:        pid,
			StartedAt:  now,
			State:      stats.State(state),
			QueueID:    currentQueueID,
			Switches:   0,
			SlotErrors: 0,
			Queues:     queueStats,
		})

		got := mm.Stats()

		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("Stats() returned unexpected difference (-want +got):\n%s", diff)
		}
	})
}

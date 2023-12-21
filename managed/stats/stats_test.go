package stats

import (
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestNewSlot(t *testing.T) {
	id := uint32(123)
	slot := New(id)

	if slot.id != id {
		t.Errorf("New slot ID = %d; want %d", slot.id, id)
	}

	if slot.state != Idle {
		t.Errorf("New slot state = %v; want %v", slot.state, Idle)
	}

	got, want := slot.pid, uint32(os.Getpid())
	if got != want {
		t.Errorf("New slot has invalid PID: got %d, want %d", got, want)
	}

	got, want = slot.startedAt, uint32(time.Now().Unix())
	if want-got > 1 {
		t.Errorf("New slot has invalid StartedAt: got %d, want ~ %d", got, want)
	}

	if len(slot.queues) != 0 {
		t.Errorf("New slot queues length = %d; want 0", len(slot.queues))
	}
}

func TestIncrementSlotError(t *testing.T) {
	slot := New(1)
	slot.IncrementSlotError()

	if slot.slotErrors != 1 {
		t.Errorf("Slot errors after increment = %d; want 1", slot.slotErrors)
	}
}

func TestSetQueueID(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		slot := New(1)
		queueID := uint32(1122)

		slot.SetQueueID(queueID)

		if slot.queueID != queueID {
			t.Errorf("SetQueueID = %d; want %d", slot.queueID, queueID)
		}

		if slot.switches != 1 {
			t.Error("Slot switches should be increased")
		}

		queue := slot.queue(queueID)
		got, want := queue.startedAt, uint32(time.Now().Unix())
		if want-got > 1 {
			t.Errorf("Queue has invalid startedAt: got %d, want ~ %d", got, want)
		}

		got, want = queue.current.started, uint32(1)
		if want != got {
			t.Errorf("Queue has invalid started: got %d, want %d", got, want)
		}

		got, want = queue.stoppedAt, uint32(0)
		if want-got > 1 {
			t.Errorf("Queue has invalid stoppedAt: got %d, want %d", got, want)
		}

	})

	t.Run("the same queue", func(t *testing.T) {
		slot := New(1)
		queueID := uint32(1122)

		slot.SetQueueID(queueID)
		slot.SetQueueID(queueID)

		if slot.switches != 1 {
			t.Error("Slot switches should be increased ones for the same queue")
		}
	})

	t.Run("stop previous queue", func(t *testing.T) {
		slot := New(1)
		firstQueueID := uint32(1122)
		secondQueueID := firstQueueID + 1

		slot.SetQueueID(firstQueueID)
		slot.SetQueueID(secondQueueID)

		firstQueue := slot.queues[firstQueueID]

		got, want := firstQueue.stoppedAt, uint32(time.Now().Unix())
		if want-got > 1 {
			t.Errorf("Queue has invalid stoppedAt: got %d, want ~ %d", got, want)

		}

		got, want = firstQueue.current.started, uint32(0)
		if want != got {
			t.Errorf("Queue has invalid started: got %d, want %d", got, want)
		}
		got, want = firstQueue.current.stopped, uint32(1)
		if want != got {
			t.Errorf("Queue has invalid stopped: got %d, want %d", got, want)
		}
	})

	t.Run("reset queue statistics upon reassignment", func(t *testing.T) {
		slot := New(1)
		firstQueueID := uint32(1122)
		secondQueueID := firstQueueID + 1

		slot.SetQueueID(firstQueueID)
		firstQueue := slot.queue(firstQueueID)
		firstQueue.current.processed = 1
		firstQueue.current.loops = 11
		firstQueue.current.workTime = 11.1
		firstQueue.current.waitTime = 111.1

		slot.SetQueueID(secondQueueID)
		slot.SetQueueID(firstQueueID)

		if firstQueue.current.processed != 0 {
			t.Errorf("Queue current.processed should be reset")
		}

		if firstQueue.current.loops != 0 {
			t.Errorf("Queue current.loops should be reset")
		}

		if firstQueue.current.workTime != 0 {
			t.Errorf("Queue current.workTime should be reset")
		}

		if firstQueue.current.waitTime != 0 {
			t.Errorf("Queue current.waitTime should be reset")
		}

		if firstQueue.total.processed != 0 {
			t.Errorf("Queue total.processed should be reset")
		}

		if firstQueue.total.loops != 0 {
			t.Errorf("Queue total.loops should be reset")
		}

		if firstQueue.total.workTime != 0 {
			t.Errorf("Queue total.workTime should be reset")
		}

		if firstQueue.total.waitTime != 0 {
			t.Errorf("Queue total.waitTime should be reset")
		}
	})
}

func TestSetState(t *testing.T) {
	slot := New(1)
	newState := Working

	slot.SetState(newState)

	if slot.state != newState {
		t.Errorf("SetState = %v; want %v", slot.state, newState)
	}
}

func TestIncrementQueueLoop(t *testing.T) {
	slot := New(1)
	queueID := uint32(2)

	slot.IncrementQueueLoop(queueID)

	queue := slot.queues[queueID]
	got, want := queue.current.loops, uint32(1)
	if got != want {
		t.Errorf("Queue current.loops after increment = %d; want %d", got, want)
	}
	got, want = queue.total.loops, uint32(1)
	if got != want {
		t.Errorf("Queue total.loops after increment = %d; want %d", got, want)
	}
}

func TestAddQueueWaitTime(t *testing.T) {
	slot := New(1)
	queueID := uint32(2)
	waitTime := 5.5

	slot.AddQueueWaitTime(queueID, waitTime)

	queue := slot.queues[queueID]
	got, want := queue.current.waitTime, waitTime
	if got != want {
		t.Errorf("Queue current.waitTime after call = %f; want %f", got, want)
	}
	got, want = queue.total.waitTime, waitTime
	if got != want {
		t.Errorf("Queue total.waitTime after call = %f; want %f", got, want)
	}
}

func TestAddQueueWorkTime(t *testing.T) {
	slot := New(1)
	queueID := uint32(2)
	workTime := 5.5

	slot.AddQueueWorkTime(queueID, workTime)

	queue := slot.queues[queueID]
	got, want := queue.current.workTime, workTime
	if got != want {
		t.Errorf("Queue current.workTime after call = %f; want %f", got, want)
	}
	got, want = queue.total.workTime, workTime
	if got != want {
		t.Errorf("Queue total.workTime after call = %f; want %f", got, want)
	}
}

func TestAddQueueProcessed(t *testing.T) {
	slot := New(1)
	queueID := uint32(2)
	processed := uint32(5)

	slot.AddQueueProcessed(queueID, processed)

	queue := slot.queues[queueID]
	got, want := queue.current.processed, processed
	if got != want {
		t.Errorf("Queue current.processed = %d; want %d", got, want)
	}
	got, want = queue.total.processed, processed
	if got != want {
		t.Errorf("Queue total.processed = %d; want %d", got, want)
	}
}

func TestAddQueueErrors(t *testing.T) {
	slot := New(1)
	queueID := uint32(2)
	errors := uint32(5)

	slot.AddQueueErrors(queueID, errors)

	queue := slot.queues[queueID]
	got, want := queue.current.errors, errors
	if got != want {
		t.Errorf("Queue current.errors = %d; want %d", got, want)
	}
	got, want = queue.total.errors, errors
	if got != want {
		t.Errorf("Queue total.errors = %d; want %d", got, want)
	}
}

func TestSlotStat(t *testing.T) {
	t.Run("empty slot", func(t *testing.T) {
		slotID := uint32(1)
		slot := New(slotID)

		want := SlotStat{
			ID:         slotID,
			PID:        slot.pid,
			StartedAt:  slot.startedAt,
			State:      slot.state,
			QueueID:    0,
			Switches:   0,
			SlotErrors: 0,
			Queues:     []QueueStat{},
		}

		got := slot.Stat()

		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("Stat() returned unexpected difference (-want +got):\n%s", diff)
		}
	})

	t.Run("some stats", func(t *testing.T) {
		slotID := uint32(1)
		slotSwitches := 10
		slotErrors := 100
		firstQueueID := uint32(1122)
		secondQueueID := firstQueueID + 1

		now := uint32(time.Now().Unix())
		wantFirstQueueStat := QueueStat{
			ID:        firstQueueID,
			StartedAt: now,
			StoppedAt: now,
			Current: QueueCounters{
				Processed: 1,
				Loops:     2,
				Started:   0,
				Stopped:   1,
			},
			Total: QueueCounters{
				Processed: 1,
				Loops:     2,
				Started:   1,
				Stopped:   1,
			},
		}
		wantSecondQueueStat := QueueStat{
			ID:        secondQueueID,
			StartedAt: now,
			StoppedAt: 0,
			Current: QueueCounters{
				Processed: 10,
				Loops:     1,
				Started:   1,
				Stopped:   0,
			},
			Total: QueueCounters{
				Processed: 10,
				Loops:     1,
				Started:   1,
				Stopped:   0,
			},
		}

		slot := New(slotID)

		slot.SetQueueID(firstQueueID)

		firstQueue := slot.queue(firstQueueID)
		firstQueue.addProcessed(wantFirstQueueStat.Current.Processed)
		firstQueue.incrementLoop()
		firstQueue.incrementLoop()

		slot.SetQueueID(secondQueueID)
		secondQueue := slot.queue(secondQueueID)
		secondQueue.addProcessed(wantSecondQueueStat.Current.Processed)
		secondQueue.incrementLoop()

		slot.switches = slotSwitches
		slot.slotErrors = slotErrors

		want := SlotStat{
			ID:         slotID,
			PID:        slot.pid,
			StartedAt:  slot.startedAt,
			State:      slot.state,
			QueueID:    secondQueueID,
			Switches:   slotSwitches,
			SlotErrors: slotErrors,
			Queues:     []QueueStat{wantFirstQueueStat, wantSecondQueueStat},
		}

		got := slot.Stat()

		if diff := cmp.Diff(want, got, cmpopts.SortSlices(func(a, b QueueStat) bool { return a.ID < b.ID })); diff != "" {
			t.Errorf("Stat() returned unexpected difference (-want +got):\n%s", diff)
		}

	})
}

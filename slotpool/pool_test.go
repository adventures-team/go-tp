package slotpool

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPool_New(t *testing.T) {
	t.Run("missing state", func(t *testing.T) {
		p := New(0)

		if len(p.state) == 0 {
			t.Error("missing default state")
		}
	})

	t.Run("slots_count", func(t *testing.T) {
		n := 3
		p := New(n)

		st := <-p.state

		got, want := len(st.slots), n
		if got != want {
			t.Errorf("unexpected number of slots: got %d, want %d", got, want)
		}
	})
}

func TestPool_AddSlot(t *testing.T) {
	tests := []struct {
		name     string
		initial  int
		expected int
	}{
		{
			name:     "first",
			initial:  0,
			expected: 1,
		},
		{
			name:     "append",
			initial:  1,
			expected: 2,
		},
		{
			name:     "several calls",
			initial:  0,
			expected: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New(tt.initial)

			for i := tt.initial; i < tt.expected; i++ {
				p.AddSlot()
			}

			st := <-p.state

			got, want := len(st.slots), tt.expected
			if got != want {
				t.Errorf("unexpected number of slots: got %d, want %d", got, want)
			}
		})
	}

	t.Run("startedAt is set on creation", func(t *testing.T) {
		p := New(0)
		p.AddSlot()

		st := <-p.state
		slot := st.slots[0]

		if time.Now().Unix()-slot.startedAt > 1 {
			t.Errorf("slot has unexpected startedAt value: got %d", slot.startedAt)
		}
	})
}

func TestPool_RemoveSlot(t *testing.T) {
	tests := []struct {
		name     string
		initial  int
		calls    int
		expected int
	}{
		{
			name:     "last",
			initial:  1,
			calls:    1,
			expected: 0,
		},
		{
			name:     "nothing to remote",
			initial:  0,
			calls:    1,
			expected: 0,
		},
		{
			name:     "several calls",
			initial:  5,
			calls:    4,
			expected: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New(tt.initial)

			for i := 0; i < tt.calls; i++ {
				p.RemoveSlot()
			}

			st := <-p.state

			got, want := len(st.slots), tt.expected
			if got != want {
				t.Errorf("unexpected number of slots: got %d, want %d", got, want)
			}
		})
	}

	t.Run("slot removed from the end", func(t *testing.T) {
		p := New(2)

		p.RemoveSlot()

		st := <-p.state

		got, want := len(st.slots), 1
		if got != want {
			t.Errorf("unexpected number of slots: got %d, want %d", got, want)
		}

		if _, ok := st.slots[1]; ok {
			t.Error("last slot must be removed")
		}
	})
}

func TestAssignSlot(t *testing.T) {
	t.Run("one slot", func(t *testing.T) {
		p := New(1)
		n := atomic.Int32{}
		wait := make(chan struct{})
		wg := sync.WaitGroup{}

		wg.Add(1)

		work := Work{
			ID: 0,
			Do: func() {
				n.Add(1)
				wg.Done()
				<-wait
			},
		}

		if err := p.AssignSlot(0, work); err != nil {
			t.Errorf("got unexpected error: %v", err)
		}

		runtime.Gosched()

		wg.Wait()

		nValue := n.Load()
		if nValue != 1 {
			t.Errorf("work must be executed once: got %d", nValue)
		}
	})

	t.Run("couple of slots", func(t *testing.T) {
		p := New(2)
		n := int32(0)
		wait := make(chan struct{})
		sem := make(chan struct{}, 2)

		work1 := Work{
			ID: 0,
			Do: func() {
				atomic.AddInt32(&n, 1)
				sem <- struct{}{}
				<-wait
			},
		}
		work2 := Work{
			ID: 1,
			Do: func() {
				atomic.AddInt32(&n, 10)
				sem <- struct{}{}
				<-wait
			},
		}

		if err := p.AssignSlot(0, work1); err != nil {
			t.Errorf("got unexpected error: %v", err)
		}

		if err := p.AssignSlot(1, work2); err != nil {
			t.Errorf("got unexpected error: %v", err)
		}

		runtime.Gosched()

		for i := 0; i < 2; i++ {
			<-sem
		}

		if atomic.LoadInt32(&n) != 11 {
			t.Errorf("work must be executed once: got %d", n)
		}

		wait <- struct{}{}

		wait <- struct{}{}
	})

	t.Run("work executed in loop", func(t *testing.T) {
		p := New(2)

		n := make(chan int, 1)
		n <- 0

		wait := make(chan struct{})
		sem1 := make(chan struct{}, 1)

		work1 := Work{
			ID: 0,
			Do: func() {
				v := <-n
				n <- v + 1
				sem1 <- struct{}{}
				<-wait
			},
		}

		if err := p.AssignSlot(0, work1); err != nil {
			t.Errorf("got unexpected error: %v", err)
		}

		wait <- struct{}{}

		<-sem1
		<-sem1

		v := <-n
		if v != 2 {
			t.Errorf("work must be executed twice: got %d", v)
		}
	})

	t.Run("add new work", func(t *testing.T) {
		p := New(1)
		n1 := int32(0)
		n2 := int32(0)
		wait := make(chan struct{})
		first := sync.WaitGroup{}
		second := sync.WaitGroup{}

		work1 := Work{Do: func() {
			n1++
			first.Done()
			<-wait
		}}
		work2 := Work{Do: func() {
			n2 += 10
			second.Done()
			<-wait
		}}

		first.Add(1)

		if err := p.AssignSlot(0, work1); err != nil {
			t.Errorf("got unexpected error: %v", err)
		}

		runtime.Gosched()
		first.Wait()

		second.Add(1)

		if err := p.AssignSlot(0, work2); err != nil {
			t.Errorf("got unexpected error: %v", err)
		}

		runtime.Gosched()

		wait <- struct{}{}

		second.Wait()

		if atomic.LoadInt32(&n1) != 1 {
			t.Errorf("all work must be executed: got %d", n1)
		}

		if atomic.LoadInt32(&n2) != 10 {
			t.Errorf("all work must be executed: got %d", n2)
		}
	})

	t.Run("replace pending work", func(t *testing.T) {
		p := New(1)
		wait := make(chan struct{})
		work1 := Work{ID: 1, Do: func() { <-wait }}
		work2 := Work{ID: 2, Do: work1.Do}
		work3 := Work{ID: 3, Do: work1.Do}

		_ = p.AssignSlot(0, work1)
		_ = p.AssignSlot(0, work2)
		_ = p.AssignSlot(0, work3)

		st := <-p.state
		slot := st.slots[0]
		got, want := <-slot.work, work3

		if got.ID != want.ID {
			t.Errorf("pending work must be replaced: got %v, want %v", got, want)
		}
	})

	t.Run("slot not found", func(t *testing.T) {
		p := New(0)

		err := p.AssignSlot(0, Work{})

		got, want := err, ErrSlotNotFound
		if got != want {
			t.Errorf("AssignSlot(0, Work{}) = %v, want %v", got, want)
		}
	})

	t.Run("work id is set on active", func(t *testing.T) {
		p := New(1)
		workID := uint32(100)
		wait := make(chan struct{})
		wg := sync.WaitGroup{}
		work := Work{ID: workID, Do: func() { wg.Done(); <-wait }}

		wg.Add(1)

		_ = p.AssignSlot(0, work)

		runtime.Gosched()

		wg.Wait()

		st := <-p.state
		slot := st.slots[0]

		got, want := slot.workID.Load(), int32(workID)
		if got != want {
			t.Errorf("got unexpected work id: got %v, want %v", got, want)
		}
	})

	t.Run("work id for idle slot", func(t *testing.T) {
		p := New(1)

		runtime.Gosched()

		st := <-p.state
		slot := st.slots[0]

		got, want := slot.workID.Load(), int32(0)
		if got != want {
			t.Errorf("got unexpected work id: got %v, want %v", got, want)
		}
	})
}

func TestReleaseSlot(t *testing.T) {
	t.Run("slot become idle", func(t *testing.T) {
		p := New(1)
		n := int32(0)
		wait := make(chan struct{})
		work := Work{ID: 100, Do: func() {
			atomic.AddInt32(&n, 1)
			<-wait
		}}

		_ = p.AssignSlot(0, work)
		_ = p.ReleaseSlot(0)

		runtime.Gosched()

		wait <- struct{}{}

		runtime.Gosched()

		st := <-p.state
		slot := st.slots[0]

		if atomic.LoadInt32(&n) != 1 {
			t.Errorf("work must be executed once: got %d", n)
		}

		got, want := slot.workID.Load(), int32(0)
		if got != want {
			t.Errorf("got unexpected work id: got %v, want %v", got, want)
		}
	})

	t.Run("several slots", func(t *testing.T) {
		p := New(2)
		n := int32(0)
		cond := sync.NewCond(&sync.Mutex{})

		work := Work{ID: 100, Do: func() {
			atomic.AddInt32(&n, 1)
			cond.L.Lock()
			cond.Wait()
			cond.L.Unlock()
		}}

		_ = p.AssignSlot(0, work)
		_ = p.AssignSlot(1, work)

		runtime.Gosched()
		time.Sleep(200 * time.Millisecond) // TODO: Slot workID does not always have time to change.

		_ = p.ReleaseSlot(0)
		_ = p.ReleaseSlot(1)

		runtime.Gosched()

		cond.Broadcast()
		runtime.Gosched()

		if atomic.LoadInt32(&n) != 2 {
			t.Errorf("work must be executed 2 times: got %d", n)
		}

		runtime.Gosched()
		time.Sleep(200 * time.Millisecond) // TODO: Slot workID does not always have time to change.

		st := <-p.state

		for i, slot := range st.slots {
			got, want := slot.workID.Load(), int32(0)
			if got != want {
				t.Errorf("got unexpected work id for slot %d: got %v, want %v", i, got, want)
			}
		}
	})

	t.Run("slot not found", func(t *testing.T) {
		p := New(0)

		err := p.ReleaseSlot(1)

		got, want := err, ErrSlotNotFound
		if got != want {
			t.Errorf("ReleaseSlot(1) = %v, want %v", got, want)
		}
	})

	t.Run("release twice", func(t *testing.T) {
		p := New(1)

		_ = p.ReleaseSlot(0)
		_ = p.ReleaseSlot(0)

		st := <-p.state
		slot := st.slots[0]

		if len(slot.release) != 0 {
			t.Error("ReleaseSlot must not try to release empty slot")
		}
	})
}

func TestFinish(t *testing.T) {
	p := New(10)
	n := int32(0)
	wait := make(chan struct{})
	f := func() { atomic.AddInt32(&n, 1); <-wait }

	for i := uint32(0); i < 10; i++ {
		_ = p.AssignSlot(i, Work{ID: i, Do: f})
	}

	runtime.Gosched()

	st := <-p.state
	p.state <- st

	close(wait)
	p.Finish()
	runtime.Gosched()

	if atomic.LoadInt32(&n) == 0 {
		t.Error("some work must be executed")
	}

	if _, ok := <-p.finish; ok {
		t.Error("finish channel must be closed")
	}

	if _, ok := <-p.state; ok {
		t.Error("state channel must be closed")
	}
}

func TestSlotStates(t *testing.T) {
	t.Run("no slots", func(t *testing.T) {
		p := New(0)

		states := p.SlotStates()

		if len(states) != 0 {
			t.Error("SlotStates should return empty slice")
		}
	})

	t.Run("idle slot", func(t *testing.T) {
		p := New(1)

		states := p.SlotStates()

		if len(states) != 1 {
			t.Error("SlotStates should return one slot")
		}

		state := states[0]

		got, want := state.WorkID, uint32(0)
		if got != want {
			t.Errorf("slot has invalid WorkID: got %d, want %d", got, want)
		}

		got, want = state.PendingWorkID, uint32(0)
		if got != want {
			t.Errorf("slot has invalid PendingWorkID: got %d, want %d", got, want)
		}

		got, want = state.UpdatedAt, uint32(0)
		if got != want {
			t.Errorf("slot has invalid UpldatedAt: got %d, want %d", got, want)
		}

		got, want = state.StartedAt, uint32(time.Now().Unix())
		if want-got > 1 {
			t.Errorf("slot has invalid StartedAt: got %d, want ~ %d", got, want)
		}
	})

	t.Run("one work", func(t *testing.T) {
		cond := sync.NewCond(&sync.Mutex{})
		p := New(1)
		workID := uint32(1122)

		f := func() {
			cond.L.Lock()
			cond.Wait()
			cond.L.Unlock()
		}

		_ = p.AssignSlot(0, Work{ID: workID, Do: f})

		time.Sleep(100 * time.Millisecond)

		states := p.SlotStates()

		if len(states) != 1 {
			t.Error("SlotStates should return one slot")
		}

		state := states[0]

		got, want := state.WorkID, workID
		if got != want {
			t.Errorf("slot has invalid WorkID: got %d, want %d", got, want)
		}

		got, want = state.UpdatedAt, uint32(time.Now().Unix())
		if want-got > 1 {
			t.Errorf("slot has invalid UpdatedAt: got %d, want ~ %d", got, want)
		}
	})

	t.Run("has pending work", func(t *testing.T) {
		cond := sync.NewCond(&sync.Mutex{})
		p := New(1)
		firstWorkID := uint32(1122)
		secondWorkID := firstWorkID + 1

		f := func() {
			cond.L.Lock()
			cond.Wait()
			cond.L.Unlock()
		}

		_ = p.AssignSlot(0, Work{ID: firstWorkID, Do: f})

		time.Sleep(100 * time.Millisecond)

		_ = p.AssignSlot(0, Work{ID: secondWorkID, Do: f})

		runtime.Gosched()

		time.Sleep(200 * time.Millisecond) // TODO: Slot workID does not always have time to change.

		states := p.SlotStates()

		if len(states) != 1 {
			t.Error("SlotStates should return one slot")
		}

		state := states[0]

		got, want := state.WorkID, firstWorkID
		if got != want {
			t.Errorf("slot has invalid WorkID: got %d, want %d", got, want)
		}

		got, want = state.PendingWorkID, secondWorkID
		if got != want {
			t.Errorf("slot has invalid PendingWorkID: got %d, want %d", got, want)
		}

		got, want = state.UpdatedAt, uint32(time.Now().Unix())
		if want-got > 1 {
			t.Errorf("slot has invalid UpdatedAt: got %d, want ~ %d", got, want)
		}

		cond.Broadcast()
	})

	t.Run("several slots", func(t *testing.T) {
		cond := sync.NewCond(&sync.Mutex{})
		slots := 3
		p := New(slots)

		f := func() {
			cond.L.Lock()
			cond.Wait()
			cond.L.Unlock()
		}

		for i := uint32(0); i < uint32(slots); i++ {
			_ = p.AssignSlot(i, Work{ID: i + 1, Do: f})
		}

		runtime.Gosched()

		states := p.SlotStates()

		got, want := len(states), 3
		if got != want {
			t.Errorf("SlotStates() returned unexpected number of states: got %d, want %d", got, want)
		}
	})
}

package slotpool

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrSlotNotFound = errors.New("slot not found")
)

type Work struct {
	ID uint32
	Do func()
}

type Pool struct {
	state  chan poolState
	finish chan struct{}
	wg     sync.WaitGroup
}

type poolState struct {
	slots map[uint32]*slot
}

type slot struct {
	work      chan Work
	release   chan struct{}
	workID    atomic.Int32
	startedAt int64
	updatedAt int64
}

type SlotState struct {
	StartedAt     uint32
	UpdatedAt     uint32
	WorkID        uint32
	PendingWorkID uint32
}

// New starts a pool of slotsCount slots
func New(slotsCount int) *Pool {
	p := &Pool{
		state:  make(chan poolState, 1),
		finish: make(chan struct{}),
	}

	state := poolState{slots: make(map[uint32]*slot, slotsCount)}
	p.state <- state

	for i := 0; i < slotsCount; i++ {
		p.AddSlot()
	}

	return p
}

// AddSlot adds new slot to the pool.
func (p *Pool) AddSlot() {
	st := <-p.state
	defer func() { p.state <- st }()

	count := uint32(len(st.slots))

	work := make(chan Work, 1)
	release := make(chan struct{}, 1)
	sl := &slot{
		work:      work,
		release:   release,
		startedAt: time.Now().Unix(),
	}

	st.slots[count] = sl

	p.wg.Add(1)

	go func() {
		for {
			select {
			case w := <-sl.work:
				sl.workID.Store(int32(w.ID))

				w.Do()

				select {
				case <-sl.release:
					sl.workID.Store(int32(0))
				default:
					if len(sl.work) == 0 {
						sl.work <- w
					} else {
						sl.workID.Store(int32(0))
					}
				}
			case <-p.finish:
				p.wg.Done()
				return
			}
		}
	}()
}

// RemoveSlot releases last slot and remove it.
func (p *Pool) RemoveSlot() {
	st := <-p.state
	defer func() { p.state <- st }()

	l := uint32(len(st.slots))
	if l == 0 {
		return
	}

	slotID := l - 1

	slot := st.slots[slotID]
	slot.release <- struct{}{}

	delete(st.slots, slotID)
}

// AssignSlot adds work for the slot.
//
// AssignSlot drops pending work and exectue new one after the current work is completed.
func (p *Pool) AssignSlot(id uint32, w Work) error {
	st := <-p.state
	defer func() { p.state <- st }()

	slot, ok := st.slots[id]
	if !ok {
		return ErrSlotNotFound
	}

LOOP:
	for {
		select {
		case _, ok := <-slot.work:
			if !ok {
				break LOOP
			}
		default:
			break LOOP
		}
	}

	slot.work <- w
	slot.updatedAt = time.Now().Unix()

	return nil
}

// ReleaseSlot removes work from the slot.
func (p *Pool) ReleaseSlot(id uint32) error {
	st := <-p.state
	defer func() { p.state <- st }()

	slot, ok := st.slots[id]
	if !ok {
		return ErrSlotNotFound
	}

	if slot.workID.Load() == 0 && len(slot.work) == 0 {
		return nil
	}

	slot.release <- struct{}{}
	slot.updatedAt = time.Now().Unix()

	return nil
}

// Finish stops pool
func (p *Pool) Finish() {
	st := <-p.state
	p.state <- st

	for _, slot := range st.slots {
		select {
		case slot.release <- struct{}{}:
		default:
		}
	}

	close(p.finish)
	p.wg.Wait()

	for _, slot := range st.slots {
		close(slot.release)
		close(slot.work)
	}

	<-p.state
	close(p.state)
}

func (p *Pool) SlotStates() []SlotState {
	st := <-p.state
	defer func() { p.state <- st }()

	states := make([]SlotState, 0, len(st.slots))

	for i := 0; i < len(st.slots); i++ {
		slot := st.slots[uint32(i)]
		pendingWork := uint32(0)

		select {
		case w := <-slot.work:
			pendingWork = w.ID
			slot.work <- w
		default:
		}

		states = append(states, SlotState{
			StartedAt:     uint32(slot.startedAt),
			UpdatedAt:     uint32(slot.updatedAt),
			WorkID:        uint32(slot.workID.Load()),
			PendingWorkID: pendingWork,
		})
	}

	return states
}

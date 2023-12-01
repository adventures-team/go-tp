package slotpool

import (
	"errors"
	"sync"
	"sync/atomic"
)

var (
	ErrSlotNotFound = errors.New("slot not found")
)

type Work struct {
	ID int
	Do func()
}

type Pool struct {
	state  chan poolState
	finish chan struct{}
	wg     sync.WaitGroup
}

type poolState struct {
	slots map[int]*slot
}

type slot struct {
	work    chan Work
	release chan struct{}
	workID  atomic.Int32
}

// New starts a pool of slotsCount slots
func New(slotsCount int) *Pool {
	p := &Pool{
		state:  make(chan poolState, 1),
		finish: make(chan struct{}),
	}

	state := poolState{slots: make(map[int]*slot, slotsCount)}
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

	count := len(st.slots)

	work := make(chan Work, 1)
	release := make(chan struct{}, 1)
	sl := &slot{
		work:    work,
		release: release,
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
					if len(work) == 0 {
						work <- w
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

	l := len(st.slots)
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
func (p *Pool) AssignSlot(id int, w Work) error {
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

	return nil
}

// ReleaseSlot removes work from the slot.
func (p *Pool) ReleaseSlot(id int) error {
	st := <-p.state
	defer func() { p.state <- st }()

	slot, ok := st.slots[id]
	if !ok {
		return ErrSlotNotFound
	}

	slot.release <- struct{}{}

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

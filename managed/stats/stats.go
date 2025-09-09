// Package stats provides functionality for tracking and managing statistical data related to queues and slots.
package stats

import (
	"os"
	"sync"
	"time"
)

// State represents the state of a slot.
type State uint32

const (
	Idle      State = 1
	Working   State = 2
	Switching State = 3
	Sleeping  State = 7
)

type queueCounters struct {
	processed uint32
	loops     uint32
	errors    uint32
	workTime  float64
	waitTime  float64
	started   uint32
	stopped   uint32
}

type queue struct {
	id        uint32
	startedAt uint32
	stoppedAt uint32

	current queueCounters
	total   queueCounters
}

// Slot represents a slot and its associated statistics.
type Slot struct {
	mu         sync.Mutex
	id         uint32
	pid        uint32
	startedAt  uint32
	state      State
	queueID    uint32
	switches   int
	slotErrors int
	queues     map[uint32]*queue
}

type QueueCounters struct {
	Processed uint32
	Errors    uint32
	Loops     uint32
	WorkTime  float64
	WaitTime  float64
	Started   uint32
	Stopped   uint32
}

// QueueStat represents the statistics of a queue.
type QueueStat struct {
	ID        uint32
	StartedAt uint32
	StoppedAt uint32

	Current QueueCounters
	Total   QueueCounters
}

// SlotStat represents the statistics of a slot.
type SlotStat struct {
	ID         uint32
	PID        uint32
	StartedAt  uint32
	State      State
	QueueID    uint32
	Switches   int
	SlotErrors int
	Queues     []QueueStat
}

// New creates a new slot with the given ID and initializes its parameters.
func New(id uint32) *Slot {
	return &Slot{
		id:        id,
		state:     Idle,
		pid:       uint32(os.Getpid()),
		startedAt: uint32(time.Now().Unix()),
		queues:    make(map[uint32]*queue),
	}
}

// IncrementSlotError increments the slot error count by 1.
func (s *Slot) IncrementSlotError() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.slotErrors++
}

// SetQueueID sets the queue ID for the slot and handles queue switching logic.
func (s *Slot) SetQueueID(id uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.queueID == id {
		return
	}

	if s.queueID != 0 {
		prevQ := s.queue(s.queueID)
		prevQ.stop()
	}

	if id != 0 {
		newQ := s.queue(id)
		newQ.reset()
		newQ.start()
	}

	s.switches++

	s.queueID = id
}

// SetState sets the state of the slot.
func (s *Slot) SetState(state State) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.state = state
}

// IncrementQueueLoop increments the slot loop count by 1.
func (s *Slot) IncrementQueueLoop(queueID uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := s.queue(queueID)
	q.incrementLoop()
}

// AddQueueWaitTime adds the provided time to the queue waitTime.
func (s *Slot) AddQueueWaitTime(queueID uint32, t float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := s.queue(queueID)
	q.addWaitTime(t)
}

// AddQueueWaitTime adds the provided time to the queue workTime.
func (s *Slot) AddQueueWorkTime(queueID uint32, t float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := s.queue(queueID)
	q.addWorkTime(t)
}

// AddQueueProcessed adds the provided number to the queue processed.
func (s *Slot) AddQueueProcessed(queueID, n uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := s.queue(queueID)
	q.addProcessed(n)
}

// AddQueueErrors adds the provided number to the queue errors.
func (s *Slot) AddQueueErrors(queueID, n uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := s.queue(queueID)
	q.addErrors(n)
}

// Stat returns the current statistics of the slot, including its queues.
func (s *Slot) Stat() SlotStat {
	s.mu.Lock()
	defer s.mu.Unlock()

	queues := make([]QueueStat, 0, len(s.queues))
	for _, q := range s.queues {
		queues = append(queues, QueueStat{
			ID:        q.id,
			StartedAt: q.startedAt,
			StoppedAt: q.stoppedAt,
			Current: QueueCounters{
				Processed: q.total.processed,
				Loops:     q.current.loops,
				WorkTime:  q.current.workTime,
				WaitTime:  q.current.waitTime,
				Started:   q.current.started,
				Stopped:   q.current.stopped,
			},
			Total: QueueCounters{
				Processed: q.total.processed,
				Errors:    q.total.errors,
				Loops:     q.total.loops,
				WorkTime:  q.total.workTime,
				WaitTime:  q.total.waitTime,
				Started:   q.total.started,
				Stopped:   q.total.stopped,
			},
		})

		q.reset()
	}

	ss := SlotStat{
		ID:         s.id,
		PID:        s.pid,
		StartedAt:  s.startedAt,
		State:      s.state,
		QueueID:    s.queueID,
		Switches:   s.switches,
		SlotErrors: s.slotErrors,
		Queues:     queues,
	}

	return ss
}

func (s *Slot) queue(id uint32) *queue {
	q, ok := s.queues[id]
	if !ok {
		q = newQueue(id)
		s.queues[id] = q
	}

	return q
}

func newQueue(id uint32) *queue {
	return &queue{
		id:      id,
		current: queueCounters{},
		total:   queueCounters{},
	}
}

func (q *queue) start() {
	q.startedAt = uint32(time.Now().Unix())
	q.current.started = 1
	q.total.started++
}

func (q *queue) stop() {
	q.stoppedAt = uint32(time.Now().Unix())
	q.current.started = 0
	q.current.stopped = 1
	q.total.stopped++
}

func (q *queue) reset() {
	q.current.processed = 0
	q.current.loops = 0
	q.current.errors = 0
	q.current.workTime = 0
	q.current.waitTime = 0
}

func (q *queue) incrementLoop() {
	q.current.loops++
	q.total.loops++
}

func (q *queue) addWaitTime(t float64) {
	q.current.waitTime += t
	q.total.waitTime += t
}

func (q *queue) addWorkTime(t float64) {
	q.current.workTime += t
	q.total.workTime += t
}

func (q *queue) addProcessed(n uint32) {
	q.current.processed += n
	q.total.processed += n
}

func (q *queue) addErrors(n uint32) {
	q.current.errors += n
	q.total.errors += n
}

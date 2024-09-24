package managedmaster

import (
	"os"
	"time"

	"github.com/rs/zerolog"

	"gitlab.corp.mail.ru/mygo/tp.git"
	"gitlab.corp.mail.ru/mygo/tp.git/managed/qmclient"
	"gitlab.corp.mail.ru/mygo/tp.git/managed/stats"
	"gitlab.corp.mail.ru/mygo/tp.git/slotpool"
	"gitlab.corp.mail.ru/mygo/tp.git/workerpool"
)

const (
	sentinel     = 0xFFFFFFFF
	initialDelay = 0.1
	maxDelay     = 10
	delayFactor  = 2.72
)

type Queue interface {
	IsRegistered(queueID uint16) bool
}

type Pool interface {
	AddSlot()
	AssignSlot(id uint32, w slotpool.Work) error
	Finish()
	ReleaseSlot(id uint32) error
	RemoveSlot()
	SlotStates() []slotpool.SlotState
}

type SlotStat interface {
	AddQueueWaitTime(queueID uint32, t float64)
	AddQueueWorkTime(queueID uint32, t float64)
	AddQueueProcessed(queueID, n uint32)
	AddQueueErrors(queueID, n uint32)
	IncrementQueueLoop(queueID uint32)
	IncrementSlotError()
	SetQueueID(id uint32)
	SetState(state stats.State)
	Stat() stats.SlotStat
}

type ManagedMaster struct {
	queue      Queue
	pool       Pool
	poller     workerpool.Poller
	logger     *zerolog.Logger
	delays     chan map[uint32]float64
	startedAt  uint32
	pid        uint32
	slotStats  chan map[uint32]SlotStat
	statSentAt uint32
}

func NewManagedMaster(queue Queue, pool Pool, poller workerpool.Poller, logger *zerolog.Logger) *ManagedMaster {
	delays := make(chan map[uint32]float64, 1)
	delays <- make(map[uint32]float64)

	slotStats := make(chan map[uint32]SlotStat, 1)
	stat := make(map[uint32]SlotStat)

	for i := range pool.SlotStates() {
		id := uint32(i)
		stat[id] = stats.New(id)
	}
	slotStats <- stat

	return &ManagedMaster{
		queue:     queue,
		pool:      pool,
		poller:    poller,
		logger:    logger,
		delays:    delays,
		startedAt: uint32(time.Now().Unix()),
		pid:       uint32(os.Getpid()),
		slotStats: slotStats,
	}
}

func (mm *ManagedMaster) AssignSlot(slotID, queueID uint32) error {
	stat := mm.slotStat(slotID)

	if queueID == 0 {
		err := mm.pool.ReleaseSlot(slotID)
		if err != nil {
			stat.IncrementSlotError()
		}

		stat.SetQueueID(0)

		return err
	}

	if !mm.queue.IsRegistered(uint16(queueID)) {
		stat.IncrementSlotError()
		mm.logger.Err(tp.ErrUnknownQueue).Uint32("queue_id", queueID).Send()

		return tp.ErrUnknownQueue
	}

	w := slotpool.Work{ID: queueID, Do: func() { mm.work(slotID, queueID) }}

	err := mm.pool.AssignSlot(slotID, w)
	if err != nil {
		stat.IncrementSlotError()
		return err
	}

	return nil
}

func (mm *ManagedMaster) work(slotID, queueID uint32) {
	stat := mm.slotStat(slotID)

	start := time.Now()
	workTime := func() { stat.AddQueueWorkTime(queueID, time.Since(start).Seconds()) }

	stat.SetQueueID(queueID)
	stat.SetState(stats.Working)

	worker := mm.poller.Poll(uint16(queueID))

	stat.IncrementQueueLoop(queueID)

	if worker == nil {
		stat.SetState(stats.Sleeping)

		delay := mm.delay(queueID)

		stat.AddQueueWaitTime(queueID, delay)
		workTime()

		d := time.Duration(delay * float64(time.Second))
		time.Sleep(d)

		return
	}

	mm.resetDelay(queueID)
	worker.Work()
	stat.AddQueueProcessed(queueID, worker.Stat().Processed())
	stat.AddQueueErrors(queueID, worker.Stat().Errors())
	workTime()
}

func (mm *ManagedMaster) delay(queueID uint32) float64 {
	delays := <-mm.delays

	delay, ok := delays[queueID]
	if !ok || delay == 0 {
		delays[queueID] = initialDelay
		delay = initialDelay
	} else {
		delay *= delayFactor
		if delay <= maxDelay {
			delays[queueID] = delay
		} else {
			delay = maxDelay
		}
	}

	mm.delays <- delays

	return delay
}

func (mm *ManagedMaster) resetDelay(queueID uint32) {
	delays := <-mm.delays
	delays[queueID] = 0
	mm.delays <- delays
}

func (mm *ManagedMaster) slotStat(slotID uint32) SlotStat {
	ss := <-mm.slotStats
	defer func() { mm.slotStats <- ss }()

	stat, ok := ss[slotID]
	if !ok {
		stat = stats.New(slotID)
		ss[slotID] = stat
	}

	return stat
}

func (mm *ManagedMaster) Stats() qmclient.StatsData {
	slotStates := mm.pool.SlotStates()
	slotStats := make([]qmclient.SlotStat, 0, len(slotStates))

	now := uint32(time.Now().Unix())

	ss := <-mm.slotStats
	defer func() { mm.slotStats <- ss }()

	for i, slState := range slotStates {
		slotNumber := uint32(i)
		sl := ss[slotNumber]
		slStat := sl.Stat()

		state := slStat.State
		if slState.PendingWorkID != 0 {
			state = stats.Switching
		}

		var interval uint32

		if mm.statSentAt != 0 {
			interval = now - mm.statSentAt
		}

		archiveQueues := make([]qmclient.QueueExtendedStat, 0, max(len(slStat.Queues)-1, 0))

		var (
			qStat       stats.QueueStat
			overallStat qmclient.QueueStat
		)

		for _, v := range slStat.Queues {
			overallStat.Errors += v.Total.Errors
			overallStat.Processed += v.Total.Processed
			overallStat.Errors += v.Total.Errors
			overallStat.Loops += v.Total.Loops
			overallStat.WorkTime += v.Total.WorkTime
			overallStat.WaitTime += v.Total.WaitTime
			overallStat.Started += v.Total.Started
			overallStat.Stopped += v.Total.Stopped

			if v.ID == slStat.QueueID {
				qStat = v
				continue
			}

			archiveQueues = append(archiveQueues, qmclient.QueueExtendedStat{
				QueueID:   v.ID,
				Flags:     0,
				StartedAt: v.StartedAt,
				StoppedAt: v.StoppedAt,
				Current: qmclient.QueueStat{
					Processed: v.Current.Processed,
					Errors:    v.Current.Errors,
					Loops:     v.Current.Loops,
					WorkTime:  v.Current.WorkTime,
					WaitTime:  v.Current.WaitTime,
					Started:   v.Current.Started,
					Stopped:   v.Current.Stopped,
				},
				Total: qmclient.QueueStat{
					Processed: v.Total.Processed,
					Errors:    v.Total.Errors,
					Loops:     v.Total.Loops,
					WorkTime:  v.Total.WorkTime,
					WaitTime:  v.Total.WaitTime,
					Started:   v.Total.Started,
					Stopped:   v.Total.Stopped,
				},
			})
		}

		var queueStat qmclient.QueueExtendedStat

		if slState.WorkID != 0 {
			queueStat = qmclient.QueueExtendedStat{
				QueueID:   slState.WorkID,
				Flags:     0,
				StartedAt: slState.StartedAt,
				Current: qmclient.QueueStat{
					Processed: qStat.Current.Processed,
					Errors:    qStat.Current.Errors,
					Loops:     qStat.Current.Loops,
					WorkTime:  qStat.Current.WorkTime,
					WaitTime:  qStat.Current.WaitTime,
					Started:   qStat.Current.Started,
					Stopped:   qStat.Current.Stopped,
				},
				Total: qmclient.QueueStat{
					Processed: qStat.Total.Processed,
					Errors:    qStat.Total.Errors,
					Loops:     qStat.Total.Loops,
					WorkTime:  qStat.Total.WorkTime,
					WaitTime:  qStat.Total.WaitTime,
					Started:   qStat.Total.Started,
					Stopped:   qStat.Total.Stopped,
				},
			}
		}

		slotStats = append(slotStats, qmclient.SlotStat{
			UpdateTime:     slState.UpdatedAt,
			StartTime:      mm.startedAt,
			Status:         uint32(slStat.State),
			LoadErrors:     uint32(slStat.SlotErrors),
			Switches:       uint32(slStat.Switches),
			PID:            slStat.PID,
			SlotNumber:     slotNumber,
			State:          uint32(state),
			Interval:       float64(interval),
			Overall:        overallStat,
			QueueStat:      queueStat,
			NextQueueID:    slState.PendingWorkID,
			ArchivedQueues: archiveQueues,
		})
	}

	mm.statSentAt = now

	return qmclient.StatsData{
		PID:                   mm.pid,
		StartedAt:             mm.startedAt,
		LastSlotStatUpdatedAt: now,
		TotalSlots:            uint32(len(slotStats)),
		UnknownSlots:          0,
		AdminConsolePort:      sentinel,
		Slots:                 slotStats,
	}
}

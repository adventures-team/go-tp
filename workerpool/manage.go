package workerpool

import (
	"sync/atomic"
	"time"

	"gitlab.corp.mail.ru/mygo/utils.git/myatomic"
)

// Manage управляет запуском заданий. Метод не возвращается, в случае паники об этом пишется в лог, и обработка возобновляется.
func (wp *WorkerPool) Manage() {
	if !atomic.CompareAndSwapInt32(&wp.isManaged, 0, 1) {
		return
	}

	jobs := make(jobMap)
	needStop := false

	for {
		delay := myatomic.LoadDuration(&wp.idleDelay)

		if needStop {
			if len(jobs) == 0 {
				break
			}
		} else {
			delay = wp.scheduleJobs(jobs)
		}

		if delay == 0 {
			// Зачем копипаста - при наличии нескольких сработавших каналов, селект выбирает рандомный
			// (в 1.18 обещают "uniform pseudo-random", в одной из предыдущих версий было "round-robin").
			// Можно было бы передать сработавший (по таймеру или фейковый) канал вместо timer.C, но тогда был бы
			// шанс выбора этого события и пропуска какого-то другого, более информативного в данной ситуации.
			// Поэтому - копипаста с дефолтом.
			select {
			case swc := <-wp.setWorkerCountChan:
				wp.setWorkerCount(jobs, swc)

			case hasJob := <-wp.hasJobChan:
				wp.onHasJob(jobs, hasJob)

			case jobID := <-wp.doneChan:
				wp.onJobDone(jobs, jobID)

			case <-wp.stopChan:
				needStop = jobs.requestStop()

			default:
			}
		} else {
			timer := time.NewTimer(delay)

			select {
			case swc := <-wp.setWorkerCountChan:
				wp.setWorkerCount(jobs, swc)

			case hasJob := <-wp.hasJobChan:
				wp.onHasJob(jobs, hasJob)

			case jobID := <-wp.doneChan:
				wp.onJobDone(jobs, jobID)

			case <-wp.stopChan:
				needStop = jobs.requestStop()

			case <-timer.C:
			}

			timer.Stop()
		}
	}
}

// scheduleJobs пытается запустить новых воркеров, если есть свободные слоты.
// возвращает задержку следующей попытки запуска - не ранее самого первого времени задержки.
// по умолчанию - idleDelay. в любом случае, при завершении какой-то работы произойдёт решедулинг, эта задержка делается, если слоты не освободятся
func (wp *WorkerPool) scheduleJobs(jobs jobMap) time.Duration {
	now := time.Now()
	delayUntil := time.Time{}

	for jobID, job := range jobs {
		if job.running >= job.requested { // все слоты заняты
			continue
		}

		if now.Before(job.delayedUntil) {
			// если не найдём никакую работу для выполнения - спим до самого раннего delayedUntil
			if delayUntil.IsZero() || delayUntil.After(job.delayedUntil) {
				delayUntil = job.delayedUntil
			}

			continue
		}

		wp.jobChan <- jobID
		job.running++

		if job.dryCount > atomic.LoadInt32(&wp.maxDryPolls) {
			// если мы запустили работу - но сейчас dryCount больше порога - устанавливаем задержку.
			// т.е. поллим по 1 заданию, даже если есть свободные воркеры.
			job.delayedUntil = time.Now().Add(myatomic.LoadDuration(&wp.dryPollDelay))

			if delayUntil.IsZero() || delayUntil.After(job.delayedUntil) {
				delayUntil = job.delayedUntil
			}
		} else {
			// если же лимит не достигнут - продолжаем интенсивный поллинг (с нулевой задержкой)
			delayUntil = now

			// при этом всё равно увеличиваем счётчик холостых заданий - чтобы не забивать воркеры зря.
			// предполагается, что поллинг - существенно быстрее обработки, поэтому если задание прилетит -
			// счётчик будет сброшен в onHasJob
			job.dryCount++
		}

		jobs[jobID] = job // https://github.com/golang/go/issues/9926
	}

	if !delayUntil.IsZero() {
		return delayUntil.Sub(now)
	}

	return myatomic.LoadDuration(&wp.idleDelay)
}

func (wp *WorkerPool) onHasJob(jobs jobMap, hasJob hasJobInfo) {
	job, ok := jobs[hasJob.jobID]

	if !ok {
		wp.logger.Error().Uint16(wp.Poller.EntityIDName(), hasJob.jobID).Msg("unexpected job poll")
		return
	}

	if hasJob.hasJob {
		job.delayedUntil = time.Time{}
		job.dryCount = 0
	} else {
		job.dryCount++

		if job.dryCount > atomic.LoadInt32(&wp.maxDryPolls) {
			// TODO вот тут неявно присутствует нулевая задержка между пустыми поллами -
			// если иф не сработал, delayedUntil остаётся 0. Учесть это при рефакторинге
			// на ненулевую задержку.
			job.delayedUntil = time.Now().Add(myatomic.LoadDuration(&wp.dryPollDelay))
		}
	}

	jobs[hasJob.jobID] = job
}

func (wp *WorkerPool) onJobDone(jobs jobMap, jobID JobTypeID) {
	job, ok := jobs[jobID]

	if !ok {
		wp.logger.Error().Uint16(wp.Poller.EntityIDName(), jobID).Msg("unexpected job completion")
		return
	}

	job.running--
	if job.running <= 0 && job.requested <= 0 {
		delete(jobs, jobID) // см.коммент в setWorkerCount, + удалением записи страхуемся от ухода в отрицательные значения при паниках в странных местах
		return
	}

	// наверно, интенсивный поллинг не нужен, для этого есть другой механизм
	// job.delayedUntil = time.Time{}
	// job.dryCount = 0

	jobs[jobID] = job
}

func (jobs jobMap) requestStop() bool {
	for jobID, job := range jobs {
		job.requested = 0
		jobs[jobID] = job
	}

	// события на выход воркерам не посылаем - зачем экономить какую-то память, если через неск. мсек процесс выйдет

	return true
}

func (wp *WorkerPool) setWorkerCount(jobs jobMap, swc setWorkerCountReq) {
	job, ok := jobs[swc.jobID]

	if !ok {
		if swc.count == 0 {
			return
		}

		jobs[swc.jobID] = jobType{
			requested: swc.count,
			running:   0,
		}
	} else { // jobID известен
		if swc.count == 0 && job.running == 0 {
			delete(jobs, swc.jobID) // если запущенные воркеры есть - запись удаляем в onJobDone, когда последний воркер завершит работу
		} else {
			if job.requested == swc.count {
				return
			}

			job.requested = swc.count
			jobs[swc.jobID] = job
		}
	}

	wp.growOrShrink(jobs)
}

func (wp *WorkerPool) growOrShrink(jobs jobMap) {
	newCount := 0

	for _, v := range jobs {
		newCount += v.requested
	}

	wp.lock.Lock()
	defer wp.lock.Unlock()

	grow := newCount - wp.goroutinesRunning
	wp.goroutinesRequired = newCount // отсутствие изменений по отношению к текущему кол-ву ещё не означает, что сохранённое кол-во не изменилось

	if grow == 0 {
		return
	}

	if grow < 0 {
		go func() { // лока уже нет, не добавлять сюда какую-то работу с залоченными данными )
			for i := 0; i > grow; i-- {
				wp.checkExitChan <- struct{}{}
			}
		}()

		return
	}

	wp.goroutinesRunning += grow

	for i := 0; i < grow; i++ {
		go wp.workerGoroutine()
	}
}

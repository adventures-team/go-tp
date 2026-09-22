package workerpool

import (
	"context"
	"log/slog"
	"time"

	advslog "github.com/adventures-team/go-adv-slog"
)

func (wp *WorkerPool) needShrink() bool {
	wp.lock.Lock()
	defer wp.lock.Unlock()

	needShrink := wp.goroutinesRunning > wp.goroutinesRequired

	if needShrink {
		wp.goroutinesRunning--
	}

	return needShrink
}

func (wp *WorkerPool) workerGoroutine() {
	defer func() {
		// тут перехватываем только паники в коде WorkerPool - в "прикладном" коде ("работы"/очереди) должна быть отдельная обработка, с учётом семантики (логгим контекст - id очередей/событий и т.д.)
		if reason := recover(); reason != nil {
			wp.logger.LogAttrs(context.Background(), slog.LevelError, "panic in WorkerPool", advslog.PanicAttrs(reason)...)
			time.Sleep(InternalPanicDelay)

			go wp.workerGoroutine()
		}
	}()

	for !wp.needShrink() {
		select {
		case jobID := <-wp.jobChan:
			func() { // "лишний" defer, чтобы в случае паники всё равно ушло событие завершения работы
				defer func() {
					wp.doneChan <- jobID
				}()

				wrk := wp.Poller.Poll(jobID)
				hasJob := wrk != nil

				wp.hasJobChan <- hasJobInfo{
					jobID:  jobID,
					hasJob: hasJob,
				}

				if hasJob {
					wrk.Work()
				}
			}()

		case <-wp.checkExitChan:
		}
	}
}

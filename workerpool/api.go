package workerpool

import (
	"time"
)

// SetWorkerCount устанавливает кол-во воркеров count для работы jobID.
// Если горутин для выполнения работ всех типов не хватает - новые запускаются сразу, если
// есть лишние - горутины выходят, только если есть простаивающие горутины.
// Если за время выполнения работы горутины перестали быть "лишними" (вызвали SetWorkerCount с новыми параметрами),
// выходить они не будут (условие выхода проверяется по завершении работы).
func (wp *WorkerPool) SetWorkerCount(jobID JobTypeID, count int) {
	wp.setWorkerCountChan <- setWorkerCountReq{
		jobID: jobID,
		count: count,
	}
}

// SetMaxDryPolls - устанавливает максимальное кол-во пустых опросов очереди, после которых будет осуществлена задержка опроса этой очереди на dryPollDelay (default: 20 раз)
func (wp *WorkerPool) SetMaxDryPolls(maxDryPolls int) {
	wp.maxDryPolls.Store(int32(maxDryPolls))
}

// SetDryPollDelay - устанавливает задержку после maxDryPolls пустых запросов (не вернувших новые записи) (default: 50 мсек)
func (wp *WorkerPool) SetDryPollDelay(dryPollDelay time.Duration) {
	wp.dryPollDelay.Store(int64(dryPollDelay))
}

// SetIdleDelay - устанавливает задержку опроса по умолчанию, если других задержек нет (default: 5 секунд)
func (wp *WorkerPool) SetIdleDelay(idleDelay time.Duration) {
	wp.idleDelay.Store(int64(idleDelay))
}

// Stop останавливает обработку. После выхода всех работающих обработчиков событий, метод Run возвращается.
// Можно вызывать этот метод более одного раза.
func (wp *WorkerPool) Stop() {
	select {
	case wp.stopChan <- struct{}{}:
	default:
	}
}

package workerpool

import (
	"sync/atomic"
	"time"

	"gitlab.cebc.tools/go/utils.git/myatomic"
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
	atomic.StoreInt32(&wp.maxDryPolls, int32(maxDryPolls)) // на x86-* запись в int с правильным выравниванием и так атомарная, но перфекционизм требует)
}

// SetDryPollDelay - устанавливает задержку после maxDryPolls пустых запросов (не вернувших новые записи) (default: 50 мсек)
func (wp *WorkerPool) SetDryPollDelay(dryPollDelay time.Duration) {
	myatomic.StoreDuration(&wp.dryPollDelay, dryPollDelay)
}

// SetIdleDelay - устанавливает задержку опроса по умолчанию, если других задержек нет (default: 5 секунд)
func (wp *WorkerPool) SetIdleDelay(idleDelay time.Duration) {
	myatomic.StoreDuration(&wp.idleDelay, idleDelay)
}

// Stop останавливает обработку. После выхода всех работающих обработчиков событий, метод Manage возвращается.
// Можно вызывать этот метод более одного раза.
func (wp *WorkerPool) Stop() {
	select {
	case wp.stopChan <- struct{}{}:
	default:
	}
}

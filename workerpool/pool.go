/*
Package workerpool - абстрактный пул воркеров.

Для использования требуется реализовать тип с методом Poll, опрашивающий очередь заданий (реализация абстрактная, под очередью подразумевается
не обязательно queued, а также вообще любая сущность, которую можно опрашивать). Poller.Poll должен возвращать интерфейс Worker, метод Work которого
осуществляет выполнение задания.

Предполагается, что опрос - быстрый, выполнение задания - может быть медленным.
*/
package workerpool

import (
	"sync"
	"sync/atomic"
	"time"

	advmetricsset "github.com/adventures-team/go-adv-metrics/set"
	"github.com/rs/zerolog"
)

const (
	// InternalPanicDelay - задержка между итерациями внутри горутины-воркера при необработанной панике.
	InternalPanicDelay = 100 * time.Millisecond

	// DefaultMaxDryPolls - кол-во пустых запусков, после которых поллинг заданий не осуществляется в течении pollDelay.
	DefaultMaxDryPolls = 20

	// DefaultDryPollDelay - задержка поллинга, если заданий определённого типа не было maxDryPolls раз подряд.
	DefaultDryPollDelay = /* 50 */ 300 * time.Millisecond

	// DefaultIdleDelay - задержка, если других задержек нет ) защита от зависания намертво. если все воркеры заняты - новые задания не посылаются всё равно.
	// Пока проект не станет стабильным - лучше не уменьшать - так ошибки в логике станут заметны быстрее (из-за долгих засыпаний).
	DefaultIdleDelay = 5 * time.Second

	chanCapacity = 10 // ёмкость служебных каналов, просто для избежания ненужных блокировок. на это кол-во не закладываемся - блокировка не должна приводить к нежелательным последствиям (если приведёт - заводим горутину для асинхронной отправки)
)

type (
	// JobTypeID - здесь работой называется очередь, на случай выноса функцонала пула воркеров из проекта очередей.
	JobTypeID = uint16

	// Poller используется для опроса ресурса с указанным ID.
	//
	// Метод Poll должен вернуть интерфейс Worker (с деталями задания, необходимыми для работы метода Worker.Work),
	// если задание получено - либо nil, если не получено.
	//
	// Предполагается, что опрос быстрый.
	Poller interface {
		EntityIDName() string  // возвращает название ID работы (для очередей - "QueueID")
		Poll(JobTypeID) Worker // nil - заданий нет
	}

	// Stopper - если poller поддерживает также этот интерфейс, при запросе остановки вызывается метод Stop.
	//
	// Внимание - именно при _запросе_ остановки методом WorkerPool.Stop! А не при выходе из Run (после завершения обработки всех заданий)!
	Stopper interface {
		Stop()
	}

	WorkerStat interface {
		Processed() uint32
		Errors() uint32
	}

	// Worker возвращается методом Poller.Poll, метод Work осуществляет выполнение задания.
	//
	// Работа (в отличие от опроса) может быть долгой. Длительность работы ни на что не влияет (кроме самого факта занятия воркера + ожидания завершения работы при запросе остановки).
	Worker interface {
		Work()
		Stat() WorkerStat
	}

	// jobType - информация о воркерах работы одного типа (одной очереди)
	// локи/атомики не нужны - объект используется одной горутине (в функции Run)
	jobType struct {
		requested    int       // установлено методом SetWorkerCount
		running      int       // в работе
		dryCount     int32     // пустых запусков
		delayedUntil time.Time // если не 0 - не посылаем новые запросы ранее этого времени
	}

	hasJobInfo struct {
		jobID  JobTypeID
		hasJob bool
	}

	jobMap map[JobTypeID]jobType

	setWorkerCountReq struct {
		jobID JobTypeID
		count int
	}

	// WorkerPool - управляемый пул воркеров
	WorkerPool struct {
		Poller  Poller // TODO зачем оно публичное? чтобы поменяли неатомарно снаружи?
		metrics advmetricsset.Set
		logger  *zerolog.Logger

		lock               sync.Mutex // под локом: goroutinesRunning и goroutinesRequired
		goroutinesRunning  int        // общее кол-во горутин всех воркеров
		goroutinesRequired int        // требуемое кол-во горутин (сумма requested в jobTypes)

		isManaged    atomic.Bool  // запущен ли менеджер. повторный запуск менеджера сразу возвращается.
		maxDryPolls  atomic.Int32 // кол-во пустых запусков, после которых поллинг заданий не осуществляется в течении pollDelay
		dryPollDelay atomic.Int64 // time.Duration. задержка поллинга, если заданий определённого типа не было maxDryPolls раз подряд
		idleDelay    atomic.Int64 // time.Duration. задержка, если других задержек нет )

		jobChan            chan JobTypeID         // небуферизованный канал - если все горутины заняты, ждём завершения (а не раздаём задания "на будущее")
		hasJobChan         chan hasJobInfo        // посылается после вызова Poller.Poll в зависимости от наличия задания
		doneChan           chan JobTypeID         // при старте задаём какую-то ёмкость - просто для избежания блокировок воркеров. Если канал долгое время не слушался, скорее всего, всё уже зависло намертво )) (по какой-то другой причине)
		checkExitChan      chan struct{}          // будит горутину из состояния ожидания задания для проверки условия выхода (goroutinesRunning > goroutinesRequired). само получение события к выходу приводить не должно (например, за время обработки события конфигурация изменилась, и выходить уже не надо)
		stopChan           chan struct{}          // выход из Run
		setWorkerCountChan chan setWorkerCountReq // jobType хранятся внутри функции Run, запросы на изменение кол-ва передаются через канал. Без буферизации - чтобы вызов SetWorkerCount блокировался хотя бы до времени получения этих данных.
	}
)

// New создаёт новый пул воркеров
func New(poller Poller, logger *zerolog.Logger) *WorkerPool {
	wp := &WorkerPool{
		Poller:             poller,
		logger:             logger,
		jobChan:            make(chan JobTypeID),
		hasJobChan:         make(chan hasJobInfo, chanCapacity),
		doneChan:           make(chan JobTypeID, chanCapacity),
		checkExitChan:      make(chan struct{}, chanCapacity),
		stopChan:           make(chan struct{}),
		setWorkerCountChan: make(chan setWorkerCountReq, chanCapacity),
	}

	wp.maxDryPolls.Store(DefaultMaxDryPolls)
	wp.dryPollDelay.Store(int64(DefaultDryPollDelay))
	wp.idleDelay.Store(int64(DefaultIdleDelay))

	return wp
}

func (wp *WorkerPool) SetMetricsSet(ms advmetricsset.Set) {
	wp.metrics = ms
}

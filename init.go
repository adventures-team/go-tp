package tp

import (
	"context"
	"flag"

	"github.com/rs/zerolog"

	"gitlab.corp.mail.ru/mygo/tp.git/queueconfig"
	"gitlab.corp.mail.ru/mygo/tp.git/workerpool"
	"gitlab.corp.mail.ru/mygo/utils.git/mylog"
	"gitlab.corp.mail.ru/mygo/utils.git/oconf"
)

// DefaultProducerInstance - имя инстанса в онлайнконфе по умолчанию для продюсеров (веб-сервисы, и т.д.)
const DefaultProducerInstance = "producer"

// Config - конфиг для инициализации обработчика или продюсера очередей.
type Config struct {
	Config       oconf.OnlineConf // Обязательный параметр. Не стал делать чтение из флага - путь завязан на проект, т.е. на набор обрабатываемых событий, поэтому должен формироваться тем же (разработчиком или генератором), что и вызовы Register
	InstanceName string           // По умолчанию, для обработчиков читается из командной строки из флага -instance, для продюсеров - DefaultProducerInstance ("producer")
	Logger       *zerolog.Logger  // По умолчанию, вызывается mylog.InitZerolog()
}

// InitProcessor - инициализация типового обработчика очередей. Простейший случай:
//
//		func main() {
//			conf := oconf.NewSubtree(onlineconf.GetModule("TREE"), "/project/queued")
//			qm := tp.Config{Config: conf}.InitProcessor()
//			app := NewService(myServiceConfig{...})
//
//			err := qm.Register(QueueID, &QueueEvent{}, app)
//			if err != nil { zLog.Fatal().Err(err).Send() }
//
//			err = qm.Register(AnotherQueueID, &AnotherQueueEvent{}, app)
//			if err != nil { zLog.Fatal().Err(err).Send() }
//
//			qm.Manage()
//		}
//
// Параметр Config.Config обязателен. Если Config.InstanceName не задан - читается параметр командной строки -instance.
// В таком случае, flag.Parse() вызывать из main() до вызова Config.Init не следует (обходить определением глобальной переменной с флагом не стал, чтобы этот флаг не появился во всех бинарниках, импортирующих эту библиотеку).
// Logger по умолчанию инициализируется при помощи mylog.InitZerolog(), из тестов можно использовать mylog.InitTestZerolog(t).
//
// В случае ошибки инициализации происходит выход (zerolog.Logger.Fatal). Предполагается вызов из функции main.
func (conf Config) InitProcessor() *QueueManager {
	if conf.Logger == nil {
		conf.Logger = mylog.InitZerolog()
	}

	if conf.InstanceName == "" {
		instanceParam := flag.String("instance", "", "queue processor instance name")
		flag.Parse()

		if *instanceParam == "" {
			conf.Logger.Fatal().Msg("-instance parameter is required when Config.InstanceName is not set")
		}

		conf.InstanceName = *instanceParam
	}

	instanceLogger := conf.Logger.With().Str("instance", conf.InstanceName).Logger()
	ctx := instanceLogger.WithContext(context.Background())

	queue, err := Init(ctx, queueconfig.New(conf.Config, conf.InstanceName))

	if err != nil {
		instanceLogger.Fatal().Err(err).Msg("error initializing queue processor")
	}

	instanceLogger.Info().Msg("queue processor started")

	return NewQueueManager(
		queue,
		workerpool.New(queue.NewPoller(&instanceLogger), &instanceLogger),
		&instanceLogger,
	)
}

// InitProducer - инициализация типового продюсера (веб-сервис, и т.д.):
//
//	func main() {
//		conf := oconf.NewSubtree(onlineconf.GetModule("TREE"), "/project/queued")
//		queue := tp.Config{Config: conf}.InitProducer()
//
//		err := queue.Register(QueueID, &QueueEvent{}, nil)
//		if err != nil { zLog.Fatal().Err(err).Send() }
//
//		err = queue.Register(AnotherQueueID, &AnotherQueueEvent{}, nil)
//		if err != nil { zLog.Fatal().Err(err).Send() }
//
//		svc := NewService(myServiceConfig{
//			...
//			Queue: queue,
//			...
//		})
//
//		...
//
//		svc.Serve()
//	}
//
//	func (svc *Service) PostUserDataHandler(...) {
//		...
//		svc.Queue.Publish(&QueueEvent{
//			UserID: params.UserID(),
//			...
//		})
//
//		...
//	}
//
// Параметр Config.Config обязателен. Если Config.InstanceName не задан - используется DefaultProducerInstance ("producer").
// Logger по умолчанию инициализируется при помощи mylog.InitZerolog(), из тестов можно использовать mylog.InitTestZerolog(t).
//
// В случае ошибки инициализации происходит выход (zerolog.Logger.Fatal). Предполагается вызов из функции main.
func (conf Config) InitProducer() *Queue {
	if conf.Logger == nil {
		conf.Logger = mylog.InitZerolog()
	}

	if conf.InstanceName == "" {
		conf.InstanceName = DefaultProducerInstance
	}

	ctx := conf.Logger.WithContext(context.Background())
	queue, err := Init(ctx, queueconfig.New(conf.Config, conf.InstanceName))

	if err != nil {
		conf.Logger.Fatal().Err(err).Msg("error initializing queue processor")
	}

	return queue
}

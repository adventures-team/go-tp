package tp

import (
	"context"
	"flag"

	"github.com/rs/zerolog"

	"gitlab.cebc.tools/go/tp.git/queueconfig"
	"gitlab.cebc.tools/go/tp.git/workerpool"
	"gitlab.cebc.tools/go/utils.git/mylog"
)

// DefaultProducerInstance - имя инстанса в онлайнконфе по умолчанию для продюсеров (веб-сервисы, и т.д.)
const DefaultProducerInstance = "producer"

// InitOptions - тип напрямую использовать не надо. Не сделан приватным для упрощения написания тестов.
type InitOptions struct {
	instanceName string
	logger       *zerolog.Logger
}

// InitOption - [WithInstanceName], [WithLogger]
type InitOption func(*InitOptions)

// WithInstanceName устанавливает имя инстанса.
//
// По умолчанию, для обработчиков читается из командной строки из флага -instance, для продюсеров - DefaultProducerInstance ("producer")
func WithInstanceName(instanceName string) InitOption {
	return func(opt *InitOptions) {
		opt.instanceName = instanceName
	}
}

// WithLogger устанавливает логгер.
//
// По умолчанию, вызывается [mylog.InitZerolog].
func WithLogger(logger *zerolog.Logger) InitOption {
	return func(opt *InitOptions) {
		opt.logger = logger
	}
}

// InitProcessor - инициализация типового обработчика очередей. Простейший случай:
//
//	func main() {
//		conf, err := onlineconf.OpenSubtree("TREE", "/project/queued")
//		qm := tp.InitProcessor(conf)
//		app := NewService(myServiceConfig{...})
//
//		err := qm.Register(QueueID, &QueueEvent{}, app)
//		if err != nil { zLog.Fatal().Err(err).Send() }
//
//		err = qm.Register(AnotherQueueID, &AnotherQueueEvent{}, app)
//		if err != nil { zLog.Fatal().Err(err).Send() }
//
//		qm.Manage()
//	}
//
// Опции:
//   - [WithInstanceName] - по умолчанию - читается параметр командной строки -instance (flag.Parse() из main() в этом случае вызывать до вызова InitProcessor не следует).
//   - [WithLogger] - если не указано явно, инициализируется при помощи [mylog.InitZerolog].
//
// В случае ошибки инициализации происходит выход ([zerolog.Logger.Fatal]). Предполагается вызов из функции main.
func InitProcessor[T queueconfig.OnlineConf](conf queueconfig.OnlineConfWithSubtree[T], opts ...InitOption) *StandaloneMaster {
	var opt InitOptions

	for _, f := range opts {
		f(&opt)
	}

	if opt.logger == nil {
		opt.logger = mylog.InitZerolog()
	}

	if opt.instanceName == "" {
		instanceParam := flag.String("instance", "", "queue processor instance name")

		flag.Parse() //nolint:revive

		if *instanceParam == "" {
			opt.logger.Fatal().Msg("-instance parameter is required when Config.InstanceName is not set")
		}

		opt.instanceName = *instanceParam
	}

	instanceLogger := opt.logger.With().Str("instance", opt.instanceName).Logger()
	ctx := instanceLogger.WithContext(context.Background())

	queue, err := NewQueue(ctx, queueconfig.New(conf, opt.instanceName))
	if err != nil {
		instanceLogger.Fatal().Err(err).Msg("error initializing queue processor")
	}

	instanceLogger.Info().Msg("queue processor started")

	return NewStandaloneManager(
		queue,
		workerpool.New(queue.NewPoller(&instanceLogger), &instanceLogger),
		&instanceLogger,
	)
}

// InitProducer - инициализация типового продюсера (веб-сервис, и т.д.):
//
//	func main() {
//		conf, err := onlineconf.OpenSubtree("TREE", "/project/queued")
//		queue := tp.InitProducer(conf)
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
// Опции:
//   - [WithInstanceName] - по умолчанию, `producer`
//   - [WithLogger] - если не указано явно, инициализируется при помощи [mylog.InitZerolog].
//
// В случае ошибки инициализации происходит выход ([zerolog.Logger.Fatal]). Предполагается вызов из функции main.
func InitProducer[T queueconfig.OnlineConf](conf queueconfig.OnlineConfWithSubtree[T], opts ...InitOption) Queue {
	opt := InitOptions{
		instanceName: DefaultProducerInstance,
	}

	for _, f := range opts {
		f(&opt)
	}

	if opt.logger == nil {
		opt.logger = mylog.InitZerolog()
	}

	ctx := opt.logger.WithContext(context.Background())

	queue, err := NewQueue(ctx, queueconfig.New(conf, opt.instanceName))
	if err != nil {
		opt.logger.Fatal().Err(err).Msg("error initializing queue processor")
	}

	return queue
}

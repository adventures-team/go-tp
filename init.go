package tp

import (
	"context"
	"flag"
	"log/slog"

	advmetricsset "github.com/adventures-team/go-adv-metrics/set"
	advslog "github.com/adventures-team/go-adv-slog"

	"gitlab.adv.team/go/tp.git/queueconfig"
	"gitlab.adv.team/go/tp.git/workerpool"
)

// DefaultProducerInstance - имя инстанса в онлайнконфе по умолчанию для продюсеров (веб-сервисы, и т.д.)
const DefaultProducerInstance = "producer"

// InitOptions - тип напрямую использовать не надо. Не сделан приватным для упрощения написания тестов.
type InitOptions struct {
	instanceName   string
	logger         *slog.Logger
	metrics        advmetricsset.Set
	logLevelConfig advslog.LevelConfig
}

// InitOption - [WithInstanceName], [WithLogger], [WithMetrics], [WithLogLevelConfig]
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
// По умолчанию, вызывается [advslog.Init].
func WithLogger(logger *slog.Logger) InitOption {
	return func(opt *InitOptions) {
		opt.logger = logger
	}
}

// WithMetrics подключает сбор метрик.
func WithMetrics(ms advmetricsset.Set) InitOption {
	return func(opt *InitOptions) {
		opt.metrics = ms
	}
}

// WithLogLevelConfig включает пер-пакетные уровни логгирования из указанного
// поддерева онлайнконфа ([advslog.WithLevelConfig]) - например,
// module.Subtree("/tp/go/log"). Оверрайд для пакета задаётся ключом
// "<root>/<import-path>/level", в частности "/tp/go/log/github.com/adventures-team/go-iprotoconn/level"
// задаёт уровень логгирования iproto-транспорта независимо от прикладных уровней.
//
// Правила использования:
//   - НЕ создавайте ключ "<root>/level": глобальным уровнем владеет параметр
//     log_level (применяется из applyConfig) - иначе за глобальный уровень
//     начнут конкурировать два писателя;
//   - поддерево должно раздаваться с child lists (ключи "<path>/" со списками
//     детей) - без них advslog не найдёт оверрайды и не подпишется на изменения;
//   - опция действует, только когда tp сам строит логгер по умолчанию: вместе
//     с [WithLogger] она игнорируется (владелец логгера сам решает, как его собрать);
//   - [advslog.Init] выполняется один раз на процесс: если приложение уже
//     вызывало его без опций, поднастройка не применится - опция должна попасть
//     в самый первый Init.
func WithLogLevelConfig(lc advslog.LevelConfig) InitOption {
	return func(opt *InitOptions) {
		opt.logLevelConfig = lc
	}
}

// advslogOpts возвращает опции для [advslog.Init] согласно InitOptions.
func (opt *InitOptions) advslogOpts() []advslog.Option {
	if opt.logLevelConfig == nil {
		return nil
	}

	return []advslog.Option{advslog.WithLevelConfig(opt.logLevelConfig)}
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
//   - [WithLogger] - если не указано явно, инициализируется при помощи [advslog.Init].
//
// В случае ошибки инициализации происходит выход с логгированием на уровне FATAL. Предполагается вызов из функции main.
func InitProcessor[T queueconfig.OnlineConf](conf queueconfig.OnlineConfWithSubtree[T], opts ...InitOption) *StandaloneMaster {
	var opt InitOptions

	for _, f := range opts {
		f(&opt)
	}

	if opt.logger == nil {
		opt.logger = advslog.Init(opt.advslogOpts()...)
	}

	if opt.instanceName == "" {
		instanceParam := flag.String("instance", "", "queue processor instance name")

		flag.Parse() //nolint:revive

		if *instanceParam == "" {
			advslog.FatalContext(advslog.NewContext(context.Background(), opt.logger),
				"-instance parameter is required when Config.InstanceName is not set")
		}

		opt.instanceName = *instanceParam
	}

	instanceLogger := opt.logger.With("instance", opt.instanceName)
	ctx := advslog.NewContext(context.Background(), instanceLogger)

	queue, err := NewQueue(ctx, queueconfig.New(conf, opt.instanceName))
	if err != nil {
		advslog.FatalContext(ctx, "error initializing queue processor", advslog.Err(err))
	}

	pool := workerpool.New(queue.NewPoller(instanceLogger), instanceLogger)

	queue.SetMetricsSet(opt.metrics)
	pool.SetMetricsSet(opt.metrics)
	instanceLogger.Info("queue processor started")

	return NewStandaloneManager(queue, pool, instanceLogger)
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
//   - [WithLogger] - если не указано явно, инициализируется при помощи [advslog.Init].
//
// В случае ошибки инициализации происходит выход с логгированием на уровне FATAL. Предполагается вызов из функции main.
func InitProducer[T queueconfig.OnlineConf](conf queueconfig.OnlineConfWithSubtree[T], opts ...InitOption) Queue {
	opt := InitOptions{
		instanceName: DefaultProducerInstance,
	}

	for _, f := range opts {
		f(&opt)
	}

	if opt.logger == nil {
		opt.logger = advslog.Init(opt.advslogOpts()...)
	}

	ctx := advslog.NewContext(context.Background(), opt.logger)

	queue, err := NewQueue(ctx, queueconfig.New(conf, opt.instanceName))
	if err != nil {
		advslog.FatalContext(ctx, "error initializing queue processor", advslog.Err(err))
	}

	queue.SetMetricsSet(opt.metrics)

	return queue
}

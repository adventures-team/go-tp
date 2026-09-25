package queuedclient

import (
	"context"
	"fmt"
	"time"

	advmetricsset "github.com/adventures-team/go-adv-metrics/set"
	advslog "github.com/adventures-team/go-adv-slog"
	iprotoconn "github.com/adventures-team/go-iprotoconn"
	"github.com/adventures-team/go-iprotoconn/breaker"
	"github.com/adventures-team/go-iprotoconn/metrics/adv"

	"github.com/adventures-team/go-tp/queueconfig"
)

const (
	// ConfServers - список шардов queued через запятую. Используется в перле.
	ConfServers = "/queueds/multi"

	// ConfTimeout - таймаут в секундах. Поддерживается дробные значения, а также синтаксис time.ParseDuration, однако,
	// для совместимости с перлом (которым используются те же пути), этого делать не рекомендуется.
	//
	// Исторически значение ограничивает одну попытку запроса, поэтому в go-iprotoconn оно
	// отображается в /attempt-timeout (см. iprotoSource).
	ConfTimeout = "/queueds/multi/timeout"

	// DefaultTimeout - таймаут по умолчанию.
	DefaultTimeout = 2 * time.Second

	// ConfRetryDelay - задержка между повторными попытками при ошибках. Используется в перле (см. коммент про ConfTimeout)
	ConfRetryDelay = "/queueds/multi/retry_delay"

	// DefaultRetryDelay - задержка между повторными попытками при ошибках по умолчанию (в перле - 1 секунда).
	DefaultRetryDelay = 300 * time.Millisecond

	// ConfRetryCount - количество повторных попыток при ошибках.
	ConfRetryCount = "/queueds/multi/retry_count"

	// DefaultRetryCount - количество повторных попыток при ошибках по умолчанию (в перле - 3 раза).
	DefaultRetryCount = 6

	// ConfRedialInterval - задержка при разрыве соединения.
	ConfRedialInterval = "/queueds/multi/redial_interval"

	// DefaultRedialInterval - задержка при разрыве соединения по умолчанию. Это дефолтное значение в go-iprotoconn - но лучше задать явно, на случай, если там поменяют
	DefaultRedialInterval = 10 * time.Millisecond

	// ConfPingInterval - интервал пинга queued.
	ConfPingInterval = "/queueds/multi/ping_interval"

	// DefaultPingInterval - интервал пинга queued по умолчанию.
	DefaultPingInterval = 5 * time.Second

	// ConfMinPID - минимальный pid, который считаем валидным (уникальным в рамках хоста). Если pid <= указанного, считаем, что работаем в контейнере.
	//
	// Подробнее - см. функцию GetPID.
	ConfMinPID = "/queueds/multi/min_pid"

	// DefaultMinPID - если /queueds/multi/min_pid не задано в онлайнконфе.
	DefaultMinPID = 20

	// BalanceShard - распределять запросы по шардам. Только для unicast-режима. Сейчас используется round robin (может быть заменён на что-то другое, например, на вероятностное распределение).
	BalanceShard = ^uint(0)
)

var reqID uint32 // per process! не per-QueuedClient. в случае рандомных id, если последовательность будет сквозная у всех "pid", хуже от этого не станет, а вот если внутри процесса с разными объектаи будет совпадение reqID - хуже станет

// QueuedClient - низкоуровневый клиент queued.
//
// Транспорт - go-iprotoconn: шардированный кластер (один сервер = один шард queued)
// с ретраями, circuit breaker'ами per shard и метриками iproto_* (если задан
// [WithMetricsSet]). Конфигурация читается из путей /queueds/multi/* через адаптер
// iprotoSource - для обеспечения обратной совместимости с предыдущими версиями go-tp,
// вместо использования путей, используемых go-iprotoconn по умолчанию.
type QueuedClient struct {
	Config     *queueconfig.Config
	client     *iprotoconn.Client
	src        *iprotoSource // конфиг go-iprotoconn; актуальные адреса шардов - для сообщений об ошибках
	shardNum   uint          // число шардов, зафиксированное при Dial (см. iprotoSource.servers)
	metrics    advmetricsset.Set
	roundRobin uint32
	pid        uint32
}

// DialOption - опции [Dial]: [WithMetricsSet].
type DialOption func(*dialOptions)

type dialOptions struct {
	metrics advmetricsset.Set
}

// WithMetricsSet включает сбор транспортных метрик iproto_* в указанный Set.
// Реестр метрик подключается при создании клиента, поэтому Set должен быть
// известен на момент вызова [Dial] (в отличие от [QueuedClient.SetMetricsSet],
// который лишь сохраняет Set для прикладных метрик уровня очередей).
func WithMetricsSet(ms advmetricsset.Set) DialOption {
	return func(opt *dialOptions) {
		opt.metrics = ms
	}
}

// Dial создаёт клиент queued по списку серверов из онлайнконфа.
//
// Соединения устанавливаются в фоне (/eager-dial в go-iprotoconn) - Dial не ждёт
// их установки и не возвращает ошибку при недоступности серверов (как и раньше
// с RedialForever); ошибка возвращается только при пустом списке серверов или
// некорректной конфигурации.
//
// Список серверов отслеживается по подписке онлайнконфа: смена адресов в /queueds/multi
// применяется без перезапуска (соединения со старыми адресами закрываются, с новыми -
// открываются). Изменение числа шардов при этом требует перезапуска - оно игнорируется
// запущенным tp с предупреждением в лог (см. iprotoSource).
func Dial(ctx context.Context, conf *queueconfig.Config, opts ...DialOption) (*QueuedClient, error) {
	pid, err := GetPID(conf.OnlineConf)
	if err != nil {
		return nil, err
	}

	var opt dialOptions

	for _, f := range opts {
		f(&opt)
	}

	// go-iprotoconn трактует отмену ctx как немедленный сброс всех соединений;
	// исторически же ctx у Dial ограничивал только установку соединений, поэтому
	// фоновые механизмы клиента отвязываются от жизни ctx (значения - логгер - сохраняются).
	bgCtx := context.WithoutCancel(ctx)

	src, err := newIprotoSource(bgCtx, conf)
	if err != nil {
		return nil, err
	}

	clientOpts := []iprotoconn.ClientOption{
		iprotoconn.WithLogger(advslog.Ctx(ctx)),
	}

	if (opt.metrics != advmetricsset.Set{}) {
		clientOpts = append(clientOpts, iprotoconn.WithMetrics(adv.NewRegistry(opt.metrics)))
	}

	client, err := iprotoconn.NewClient(bgCtx, src, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("iproto client: %w", err)
	}

	return &QueuedClient{
		Config:   conf,
		client:   client,
		src:      src,
		shardNum: uint(len(src.servers())),
		metrics:  opt.metrics,
		pid:      pid,
	}, nil
}

// ShardNum возвращает количество шардов, указанных в /queueds/multi.
func (qc *QueuedClient) ShardNum() uint {
	return qc.shardNum
}

// addr возвращает актуальный адрес шарда (нумерация с 0) - для сообщений об ошибках.
func (qc *QueuedClient) addr(shard uint) string {
	addrs := qc.src.servers()
	if shard >= uint(len(addrs)) {
		return ""
	}

	return addrs[shard]
}

func (qc *QueuedClient) MetricsSet() advmetricsset.Set {
	return qc.metrics
}

// SetMetricsSet сохраняет Set для прикладных метрик (tp_events_* и т.д., см. Queue).
// Транспортные метрики iproto_* этим методом не включаются - реестр подключается
// только при создании клиента опцией [WithMetricsSet].
func (qc *QueuedClient) SetMetricsSet(ms advmetricsset.Set) {
	qc.metrics = ms
}

// IsHealthy сообщает о работоспособности кластера queued с точки зрения клиента:
// ни у одного шарда не открыт circuit breaker, и в каждом шарде есть пригодный
// (не замороженный после ошибок) сервер.
//
// Соединения устанавливаются по требованию и в фоне, поэтому "холодный" клиент
// сразу после Dial считается здоровым - метод отражает именно наличие проблем,
// а не факт установленных TCP-соединений.
func (qc *QueuedClient) IsHealthy() bool {
	usable := make(map[int]bool, qc.shardNum)
	brokenShards := make(map[int]bool)

	for _, row := range qc.client.ClusterState() {
		if row.Breaker == breaker.Open {
			brokenShards[row.Shard] = true
		}

		if !row.Frozen {
			usable[row.Shard] = true
		}
	}

	for shard := 1; shard <= int(qc.shardNum); shard++ {
		if brokenShards[shard] || !usable[shard] {
			return false
		}
	}

	return true
}

// IsOnline проверяет доступность кластера queued.
//
// Deprecated: метод больше не проверяет наличие живых TCP-соединений со всеми
// шардами (go-iprotoconn устанавливает соединения по требованию и в фоне) -
// он эквивалентен [QueuedClient.IsHealthy], который и следует использовать.
func (qc *QueuedClient) IsOnline() bool {
	return qc.IsHealthy()
}

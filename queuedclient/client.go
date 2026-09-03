package queuedclient

import (
	"context"
	"fmt"
	"time"

	advmetricsset "github.com/my-mail-ru/go-adv-metrics/set"

	easygo "gitlab.adv.team/go/easygo.git/iproto"
	"gitlab.adv.team/go/tp.git/queueconfig"
	"gitlab.adv.team/go/utils.git/mylog"
)

const (
	// ConfServers - список шардов queued через запятую. Используется в перле.
	ConfServers = "/queueds/multi"

	// ConfTimeout - таймаут в секундах. Поддерживается дробные значения, а также синтаксис time.ParseDuration, однако,
	// для совместимости с перлом (которым используются те же пути), этого делать не рекомендуется.
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

	// DefaultRedialInterval - задержка при разрыве соединения по умолчанию. Это дефолтное значение в iproto - но лучше задать явно, на случай, если там поменяют
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

// QueuedClient - низкоуровневый клиент queued
type QueuedClient struct {
	Config     *queueconfig.Config
	shards     []*easygo.Pool
	metrics    advmetricsset.Set
	roundRobin uint32
	pid        uint32
}

// Dial открывает соединения к серверам queued, указанным в онлайнконфе
func Dial(ctx context.Context, conf *queueconfig.Config) (*QueuedClient, error) {
	pid, err := GetPID(conf.OnlineConf)
	if err != nil {
		return nil, err
	}

	timeout := conf.GetDuration(ctx, ConfTimeout, DefaultTimeout)
	redialInterval := conf.GetDuration(ctx, ConfRedialInterval, DefaultRedialInterval)
	pingInterval := conf.GetDuration(ctx, ConfPingInterval, DefaultPingInterval)

	serverStrings := queueconfig.CSVRegex.Split(conf.GetString(ConfServers, ""), -1)
	shards := make([]*easygo.Pool, 0, len(serverStrings))

	for _, server := range serverStrings {
		if server == "" {
			continue
		}

		logAdapter := mylog.IprotoLogger{}

		endpoint, err := easygo.Dial(ctx, "tcp", server, &easygo.PoolConfig{
			Size:              1,
			ConnectTimeout:    timeout,
			DialTimeout:       timeout,
			RedialInterval:    redialInterval,
			MaxRedialInterval: redialInterval,
			RedialForever:     true,
			Logger:            logAdapter,
			ChannelConfig: &easygo.ChannelConfig{
				WriteTimeout:   timeout,
				RequestTimeout: timeout,
				PingInterval:   pingInterval,
				Logger:         logAdapter,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("Dial %s: %w", server, err)
		}

		shards = append(shards, endpoint)
	}

	if len(shards) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNoServers, ConfServers)
	}

	return &QueuedClient{
		Config: conf,
		shards: shards,
		pid:    pid,
	}, nil
}

// ShardNum возвращает количество шардов, указанных в /queueds/multi.
func (qc *QueuedClient) ShardNum() uint {
	return uint(len(qc.shards))
}

func (qc *QueuedClient) MetricsSet() advmetricsset.Set {
	return qc.metrics
}

func (qc *QueuedClient) SetMetricsSet(ms advmetricsset.Set) {
	qc.metrics = ms
}

// IsOnline проверяет наличие соединения со всеми шардами.
func (qc *QueuedClient) IsOnline() bool {
	online := 0

	for _, shard := range qc.shards {
		if shard.Stats().Online > 0 {
			online++
		}
	}

	return online == len(qc.shards)
}

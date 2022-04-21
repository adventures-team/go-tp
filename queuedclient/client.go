package queuedclient

import (
	"context"
	"fmt"
	"time"

	"gitlab.corp.mail.ru/e-mail-ru/easygo/iproto"

	"gitlab.corp.mail.ru/mygo/tp.git/queueconfig"
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
	DefaultRetryDelay = 100 * time.Millisecond

	// ConfRetryCount - количество повторных попыток при ошибках.
	ConfRetryCount = "/queueds/multi/retry_count"

	// DefaultRetryCount - количество повторных попыток при ошибках по умолчанию.
	DefaultRetryCount = 3

	// ConfRedialInterval - задержка при разрыве соединения.
	ConfRedialInterval = "/queueds/multi/redial_interval"

	// DefaultRedialInterval - задержка при разрыве соединения по умолчанию. Это дефолтное значение в iproto - но лучше задать явно, на случай, если там поменяют
	DefaultRedialInterval = 10 * time.Millisecond

	// ConfPingInterval - интервал пинга queued.
	ConfPingInterval = "/queueds/multi/ping_interval"

	// DefaultPingInterval - интервал пинга queued по умолчанию.
	DefaultPingInterval = 5 * time.Second

	// BalanceShard - распределять запросы по шардам. Только для unicast-режима. Сейчас используется round robin (может быть заменён на что-то другое, например, на вероятностное распределение).
	BalanceShard = ^uint(0)
)

var reqID uint32 // per process! не per-QueuedClient. в случае рандомных id, если последовательность будет сквозная у всех "pid", хуже от этого не станет, а вот если внутри процесса с разными объектаи будет совпадение reqID - хуже станет

// QueuedClient - низкоуровневый клиент queued
type QueuedClient struct {
	*queueconfig.Config
	shards     []*iproto.Pool
	roundRobin uint32
	pid        uint32
}

// Dial открывает соединения к серверам queued, указанным в онлайнконфе
func Dial(ctx context.Context, conf *queueconfig.Config) (*QueuedClient, error) {
	pid, err := GetPID(conf.OnlineConf)
	if err != nil {
		return nil, err
	}

	timeout, err := conf.GetDuration(ConfTimeout, DefaultTimeout)
	if err != nil {
		return nil, err
	}

	redialInterval, err := conf.GetDuration(ConfRedialInterval, DefaultRedialInterval)
	if err != nil {
		return nil, err
	}

	pingInterval, err := conf.GetDuration(ConfPingInterval, DefaultPingInterval)
	if err != nil {
		return nil, err
	}

	serverStrings := queueconfig.CSVRegex.Split(conf.GetString(ConfServers, ""), -1)
	shards := make([]*iproto.Pool, 0, len(serverStrings))

	for _, server := range serverStrings {
		if server == "" {
			continue
		}

		endpoint, err := iproto.Dial(ctx, "tcp", server, &iproto.PoolConfig{
			Size:              1,
			ConnectTimeout:    timeout,
			DialTimeout:       timeout,
			RedialInterval:    redialInterval,
			MaxRedialInterval: redialInterval,
			RedialForever:     true,
			Logger:            IprotoLogger{},
			ChannelConfig: &iproto.ChannelConfig{
				WriteTimeout:   timeout,
				RequestTimeout: timeout,
				PingInterval:   pingInterval,
				Logger:         IprotoLogger{},
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

func (qc *QueuedClient) ShardNum() uint {
	return uint(len(qc.shards))
}

package queuedclient

import (
	"context"
	"fmt"
	"time"

	"gitlab.corp.mail.ru/e-mail-ru/easygo/iproto"

	"gitlab.corp.mail.ru/mygo/tp.git/queueconfig"
)

const (
	confPathServer  = "/queueds/main"
	confPathTimeout = "/queueds/main/timeout"

	defaultTimeout        = 2                     // seconds TODO onlineconf.GetDuration
	defaultRedialInterval = 10 * time.Millisecond // это дефолтное значение в iproto - но лучше задать явно, на случай, если там поменяют
	defaultPingInterval   = 5 * time.Second

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
	pid, err := getPID(conf.OnlineConf)
	if err != nil {
		return nil, err
	}

	timeout := time.Duration(conf.GetInt(confPathTimeout, defaultTimeout)) * time.Second
	servers := conf.GetString(confPathServer, "")
	serverStrings := queueconfig.CSVRegex.Split(servers, -1)
	shards := make([]*iproto.Pool, 0, len(serverStrings))

	for _, server := range serverStrings {
		if server == "" {
			continue
		}

		endpoint, err := iproto.Dial(ctx, "tcp", server, &iproto.PoolConfig{
			Size:              1,
			ConnectTimeout:    timeout,
			DialTimeout:       timeout,
			RedialInterval:    defaultRedialInterval,
			MaxRedialInterval: defaultRedialInterval,
			RedialForever:     true,
			Logger:            IprotoLogger{},
			ChannelConfig: &iproto.ChannelConfig{
				WriteTimeout:   timeout,
				RequestTimeout: timeout,
				PingInterval:   defaultPingInterval,
				Logger:         IprotoLogger{},
			},
		})

		if err != nil {
			return nil, fmt.Errorf("Dial %s: %w", server, err)
		}

		shards = append(shards, endpoint)
	}

	if len(shards) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNoServers, confPathServer)
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

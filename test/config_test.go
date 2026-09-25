//go:build integration

package tptest

import (
	"log/slog"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	"github.com/adventures-team/go-tp"
	"github.com/adventures-team/go-tp/queueconfig"
	"github.com/adventures-team/go-tp/queuedclient"
	mockonlineconf "github.com/adventures-team/go-tp/test/mock/onlineconf"
)

var (
	// неатомарные - менять только перед вызовом Manage
	confLogLevel       = logLevelDebug
	confFetchBatchSize = 100

	// читать/писать только атомарными операциями
	confOOMSoftLimit       atomic.Value
	confOOMHardLimit       atomic.Value
	confWorkers            int32
	confWorkersMax         int32
	confWorkersMaxPerQueue int32
)

var reloadChan = make(chan struct{}, 1)

func resetConfig() {
	confOOMSoftLimit.Store("100Gb")
	confOOMHardLimit.Store("100Gb")
	atomic.StoreInt32(&confWorkers, workersDefault)
	atomic.StoreInt32(&confWorkersMax, workersMaxDefault)
	atomic.StoreInt32(&confWorkersMaxPerQueue, wokersMaxPerQueueDefault)
}

func setConfInt32(name string, conf *int32, val int32, delay time.Duration) {
	slog.Warn("setting config limit", "name", name, "value", val)
	atomic.StoreInt32(conf, val)
	reloadConfig()
	time.Sleep(delay)
}

func setWorkersMax(val int32, delay time.Duration) {
	setConfInt32("workers_max", &confWorkersMax, val, delay)
}

func setWorkersMaxPerQueue(val int32, delay time.Duration) {
	setConfInt32("workers_max_per_queue", &confWorkersMaxPerQueue, val, delay)
}

var confMissingStrings = []string{
	queuedclient.ConfTimeout,
	queuedclient.ConfRetryDelay,
	queuedclient.ConfRedialInterval,
	queuedclient.ConfPingInterval,
}

func newMockConf(t *testing.T, queueID tp.QueueIDType) OnlineConfWithSubtree {
	resetConfig()

	conf := mockonlineconf.NewMockOnlineConfWithSubtree(gomock.NewController(t))
	qstr := strconv.Itoa(int(queueID))
	qprefix := "/" + qstr + "/"

	// queuedclient
	conf.EXPECT().GetInt(queuedclient.ConfMinPID, queuedclient.DefaultMinPID).AnyTimes().Return(queuedclient.DefaultMinPID)
	conf.EXPECT().GetString(queuedclient.ConfServers, "").AnyTimes().Return(testShard0Addr + ", " + testShard1Addr)
	conf.EXPECT().GetInt(queuedclient.ConfRetryCount, queuedclient.DefaultRetryCount).AnyTimes().Return(queuedclient.DefaultRetryCount)
	// подписка адаптера go-iprotoconn на список серверов (горячее переподключение); в моке не срабатывает
	conf.EXPECT().SubscribeChan(queuedclient.ConfServers, gomock.Any()).AnyTimes().Return(nil)
	conf.EXPECT().UnsubscribeChan(queuedclient.ConfServers, gomock.Any()).AnyTimes()

	for _, confPath := range confMissingStrings {
		conf.EXPECT().GetStringIfExists(confPath).AnyTimes().Return("", false)
	}

	// global
	gconf := mockonlineconf.NewMockOnlineConf(gomock.NewController(t))

	conf.EXPECT().Subtree("/tp/go").AnyTimes().Return(gconf)

	gconf.EXPECT().Subscribe("/").AnyTimes().Return(reloadChan, nil)
	gconf.EXPECT().GetStringIfExists("/oom_soft_downscale_rate").AnyTimes().Return("-666", true)

	// per-instance
	iconf := mockonlineconf.NewMockOnlineConf(gomock.NewController(t))

	conf.EXPECT().Subtree("/tp/go/" + testInstanceName).AnyTimes().Return(iconf)

	iconf.EXPECT().GetStringIfExists("/queue_ids").AnyTimes().Return(qstr, true)
	iconf.EXPECT().GetStringIfExists("/log_level").AnyTimes().DoAndReturn(func(string) (string, bool) { return confLogLevel, true })
	iconf.EXPECT().GetStringIfExists("/oom_soft_limit").AnyTimes().DoAndReturn(func(string) (string, bool) { return confOOMSoftLimit.Load().(string), true })
	iconf.EXPECT().GetStringIfExists("/oom_hard_limit").AnyTimes().DoAndReturn(func(string) (string, bool) { return confOOMHardLimit.Load().(string), true })
	iconf.EXPECT().GetStringIfExists("/oom_soft_downscale_rate").AnyTimes().Return("", false)
	iconf.EXPECT().GetStringIfExists("/oom_check_period").AnyTimes().Return("2s", true)
	iconf.EXPECT().GetIntErr("/workers_max").AnyTimes().DoAndReturn(func(string) (int, error) { return int(atomic.LoadInt32(&confWorkersMax)), nil })
	iconf.EXPECT().GetIntErr("/workers_max_per_queue").AnyTimes().DoAndReturn(func(string) (int, error) { return int(atomic.LoadInt32(&confWorkersMaxPerQueue)), nil })

	// per-queue
	iconf.EXPECT().GetStringIfExists(qprefix+"defer_time").AnyTimes().Return("1", true)
	iconf.EXPECT().GetStringIfExists(qprefix+"defer_time_jitter").AnyTimes().Return("100ms", true) // TODO в протоколе секунды, какие 100мс )) после активного тестирования увеличить.
	iconf.EXPECT().GetStringIfExists(qprefix+"initial_delay").AnyTimes().Return("0s", true)
	iconf.EXPECT().GetIntErr(qprefix + "workers").AnyTimes().DoAndReturn(func(string) (int, error) { return int(atomic.LoadInt32(&confWorkers)), nil })
	iconf.EXPECT().GetIntErr(qprefix + "fetch_batch_size").AnyTimes().DoAndReturn(func(string) (int, error) { return confFetchBatchSize, nil })
	iconf.EXPECT().GetIntErr(qprefix+"update_batch_size").AnyTimes().Return(queueconfig.DefaultUpdateBatchSize, nil)
	iconf.EXPECT().GetStringIfExists(qprefix+"retry_type").AnyTimes().Return("exp", true)
	iconf.EXPECT().GetIntErr(qprefix+"retry_limit").AnyTimes().Return(5, nil)
	iconf.EXPECT().GetStringIfExists(qprefix+"retry_delay").AnyTimes().Return("1s", true)
	iconf.EXPECT().GetStringIfExists(qprefix+"retry_coeff").AnyTimes().Return("500ms", true)
	iconf.EXPECT().GetStringIfExists(qprefix+"retry_exp").AnyTimes().Return("2", true)

	return conf
}

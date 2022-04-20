//go:build integration
// +build integration

package tptest

import (
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"

	"gitlab.corp.mail.ru/mygo/tp.git"
	"gitlab.corp.mail.ru/mygo/tp.git/queueconfig"
	"gitlab.corp.mail.ru/mygo/utils.git/oconf"
)

const (
	workersDefault           = 30
	workersMaxDefault        = 100
	wokersMaxPerQueueDefault = 0
	workersMaxLimit          = 5
)

var (
	// неатомарные - менять только перед вызовом Manage
	confLogLevel       = "debug"
	confFetchBatchSize = 100

	// читать/писать только атомарными операциями
	confOOMSoftLimit       atomic.Value
	confOOMHardLimit       atomic.Value
	confWorkers            int32
	confWorkersMax         int32
	confWorkersMaxPerQueue int32
)

func resetConfig() {
	confOOMSoftLimit.Store("100Gb")
	confOOMHardLimit.Store("100Gb")
	atomic.StoreInt32(&confWorkers, workersDefault)
	atomic.StoreInt32(&confWorkersMax, workersMaxDefault)
	atomic.StoreInt32(&confWorkersMaxPerQueue, wokersMaxPerQueueDefault)
}

func init() {
	resetConfig()
}

func newMockConf(t *testing.T, queueID tp.QueueIDType) oconf.OnlineConf {
	conf := NewMockOnlineConf(gomock.NewController(t))
	qstr := strconv.Itoa(int(queueID))
	iprefix := "/tp/go/" + testInstanceName + "/"
	qprefix := iprefix + qstr + "/"

	// queuedclient
	conf.EXPECT().GetString("/queueds/main", "").AnyTimes().Return("127.0.0.1:20000")
	conf.EXPECT().GetInt("/queueds/main/min_pid", 20).AnyTimes().Return(20)
	conf.EXPECT().GetInt("/queueds/main/timeout", 2).AnyTimes().Return(2)

	// per-instance
	conf.EXPECT().GetStringIfExists(iprefix+"queue_ids").AnyTimes().Return(qstr, true)
	conf.EXPECT().GetStringIfExists(iprefix + "log_level").AnyTimes().DoAndReturn(func(string) (string, bool) { return confLogLevel, true })
	conf.EXPECT().GetStringIfExists(iprefix + "oom_soft_limit").AnyTimes().DoAndReturn(func(string) (string, bool) { return confOOMSoftLimit.Load().(string), true })
	conf.EXPECT().GetStringIfExists(iprefix + "oom_hard_limit").AnyTimes().DoAndReturn(func(string) (string, bool) { return confOOMHardLimit.Load().(string), true })
	conf.EXPECT().GetStringIfExists(iprefix+"oom_soft_downscale_rate").AnyTimes().Return("", false)
	conf.EXPECT().GetStringIfExists("/tp/go/oom_soft_downscale_rate").AnyTimes().Return("", false)
	conf.EXPECT().GetStringIfExists(iprefix+"oom_check_period").AnyTimes().Return("2s", true)
	conf.EXPECT().GetIntIfExists(iprefix + "workers_max").AnyTimes().DoAndReturn(func(string) (int, bool) { return int(atomic.LoadInt32(&confWorkersMax)), true })
	conf.EXPECT().GetIntIfExists(iprefix + "workers_max_per_queue").AnyTimes().DoAndReturn(func(string) (int, bool) { return int(atomic.LoadInt32(&confWorkersMaxPerQueue)), true })

	// per-queue
	conf.EXPECT().GetStringIfExists(qprefix+"defer_time").AnyTimes().Return("1", true)
	conf.EXPECT().GetStringIfExists(qprefix+"defer_time_jitter").AnyTimes().Return("100ms", true) // TODO в протоколе секунды, какие 100мс )) после активного тестирования увеличить.
	conf.EXPECT().GetStringIfExists(qprefix+"initial_delay").AnyTimes().Return("0s", true)
	conf.EXPECT().GetIntIfExists(qprefix + "workers").AnyTimes().DoAndReturn(func(string) (int, bool) { return int(atomic.LoadInt32(&confWorkers)), true })
	conf.EXPECT().GetIntIfExists(qprefix + "fetch_batch_size").AnyTimes().DoAndReturn(func(string) (int, bool) { return confFetchBatchSize, true })
	conf.EXPECT().GetIntIfExists(qprefix+"update_batch_size").AnyTimes().Return(queueconfig.DefaultUpdateBatchSize, true)

	return conf
}

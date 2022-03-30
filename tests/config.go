//go:build integration
// +build integration

package tptest

import (
	"path"
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"

	"gitlab.corp.mail.ru/mygo/tp.git"
	"gitlab.corp.mail.ru/mygo/tp.git/queueconfig"
	"gitlab.corp.mail.ru/mygo/utils.git/oconf"
)

var (
	confLogLevel     = "debug"
	confOOMSoftLimit = "100Gb"
	confOOMHardLimit = "100Gb"
)

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
	conf.EXPECT().GetStringIfExists(path.Join("/tp/go", testInstanceName, "queue_ids")).AnyTimes().Return(qstr, true)
	conf.EXPECT().GetStringIfExists(iprefix + "log_level").AnyTimes().DoAndReturn(func(string) (string, bool) { return confLogLevel, true })
	conf.EXPECT().GetStringIfExists(iprefix + "oom_soft_limit").AnyTimes().DoAndReturn(func(string) (string, bool) { return confOOMSoftLimit, true })
	conf.EXPECT().GetStringIfExists(iprefix + "oom_hard_limit").AnyTimes().DoAndReturn(func(string) (string, bool) { return confOOMHardLimit, true })
	conf.EXPECT().GetStringIfExists(iprefix+"oom_check_period").AnyTimes().Return("2s", true)
	conf.EXPECT().GetIntIfExists(iprefix+"workers_max").AnyTimes().Return(100, true)
	conf.EXPECT().GetIntIfExists(iprefix+"workers_max_per_queue").AnyTimes().Return(0, true)

	// per-queue
	conf.EXPECT().GetStringIfExists(qprefix+"defer_time").AnyTimes().Return("1s", true)
	conf.EXPECT().GetStringIfExists(qprefix+"defer_time_jitter").AnyTimes().Return("100ms", true)
	conf.EXPECT().GetStringIfExists(qprefix+"initial_delay").AnyTimes().Return("0s", true)
	conf.EXPECT().GetIntIfExists(qprefix+"workers").AnyTimes().Return(30, true)
	conf.EXPECT().GetIntIfExists(qprefix+"fetch_batch_size").AnyTimes().Return(100, true)
	conf.EXPECT().GetIntIfExists(qprefix+"update_batch_size").AnyTimes().Return(queueconfig.DefaultUpdateBatchSize, true)

	return conf
}

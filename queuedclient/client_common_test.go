//go:build integration || stress
// +build integration stress

package queuedclient_test

import (
	"testing"
	"time"

	"github.com/onlineconf/onlineconf-go"
	zLog "github.com/rs/zerolog/log"

	"gitlab.corp.mail.ru/mygo/tp.git/queueconfig"
	"gitlab.corp.mail.ru/mygo/tp.git/queuedclient"
	"gitlab.corp.mail.ru/mygo/utils.git/oconf"
)

const (
	testConfPrefix   = "/project/queued"
	testInstanceName = "test"
	testQueueID      = 65534
	testBatchSize    = 10
	testDefer        = 2 * time.Second
	testPollDelay    = time.Second
)

func getTestClient(t *testing.T) *queuedclient.QueuedClient {
	conf := oconf.NewSubtree(onlineconf.GetModule("TREE"), testConfPrefix)
	queueConf := queueconfig.New(conf, testInstanceName)
	client, err := queuedclient.New(queueConf, &zLog.Logger)

	if err != nil {
		t.Fatal(err)
	}

	return client
}

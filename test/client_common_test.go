//go:build integration || stress
// +build integration stress

package tptest

import (
	"context"
	"sync"
	"testing"

	"github.com/onlineconf/onlineconf-go"

	"gitlab.cebc.tools/go/tp.git/queueconfig"
	"gitlab.cebc.tools/go/tp.git/queuedclient"
	"gitlab.cebc.tools/go/utils.git/mylog"
	"gitlab.cebc.tools/go/utils.git/oconf"
)

var initConfig sync.Once

func getTestClient(t *testing.T) (context.Context, *queuedclient.QueuedClient) {
	ctx := mylog.InitTestZerolog(t).WithContext(context.Background())

	//	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	initConfig.Do(func() {
		configDir := ConfigDir()
		onlineconf.Initialize(configDir)
	})

	conf := oconf.NewSubtree(onlineconf.GetModule("TREE"), testConfPrefix)
	queueConf := queueconfig.New(conf, testInstanceName)
	client, err := queuedclient.Dial(ctx, queueConf)

	if err != nil {
		t.Fatal(err)
	}

	return ctx, client
}

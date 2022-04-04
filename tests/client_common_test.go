//go:build integration || stress
// +build integration stress

package tptest

import (
	"context"
	"testing"

	"github.com/onlineconf/onlineconf-go"

	"gitlab.corp.mail.ru/mygo/tp.git/queueconfig"
	"gitlab.corp.mail.ru/mygo/tp.git/queuedclient"
	"gitlab.corp.mail.ru/mygo/utils.git/oconf"
)

func getTestClient(t *testing.T) (context.Context, *queuedclient.QueuedClient) {
	ctx := context.Background()
	conf := oconf.NewSubtree(onlineconf.GetModule("TREE"), testConfPrefix)
	queueConf := queueconfig.New(conf, testInstanceName)
	client, err := queuedclient.Dial(ctx, queueConf)

	if err != nil {
		t.Fatal(err)
	}

	return ctx, client
}

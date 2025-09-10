//go:build integration || stress

package tptest

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/onlineconf/onlineconf-go/v2"

	"gitlab.cebc.tools/go/tp.git/queueconfig"
	"gitlab.cebc.tools/go/tp.git/queuedclient"
	"gitlab.cebc.tools/go/utils.git/mylog"
)

func getTestClient(t *testing.T) (context.Context, *queuedclient.QueuedClient) {
	ctx := mylog.InitTestZerolog(t).WithContext(context.Background())

	//	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	conf, err := onlineconf.OpenSubtree(filepath.Join(ConfigDir(), "TREE.cdb"), testConfPrefix)
	if err != nil {
		t.Fatal(err)
	}

	queueConf := queueconfig.New(conf, testInstanceName)

	client, err := queuedclient.Dial(ctx, queueConf)
	if err != nil {
		t.Fatal(err)
	}

	return ctx, client
}

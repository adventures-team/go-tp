//go:build integration || stress

package tptest

import (
	"context"
	"path"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/onlineconf/onlineconf-go/v2"

	"gitlab.adv.team/go/tp.git/queueconfig"
	"gitlab.adv.team/go/tp.git/queuedclient"
	"gitlab.adv.team/go/utils.git/mylog"
)

func getTestClient(t *testing.T) (context.Context, *queuedclient.QueuedClient) {
	ctx := mylog.InitTestZerolog(t).WithContext(context.Background())

	// zerolog.SetGlobalLevel(zerolog.DebugLevel)

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

// ConfigDir returns path to directory with TREE.cdb file.
func ConfigDir() string {
	_, b, _, _ := runtime.Caller(0)
	root := filepath.Dir(filepath.Dir(b))

	return path.Join(root, "build", "local")
}

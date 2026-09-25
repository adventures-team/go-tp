//go:build integration || stress

package tptest

import (
	"context"
	"testing"

	advslog "github.com/adventures-team/go-adv-slog"

	"github.com/onlineconf/onlineconf-go/v2"

	"github.com/adventures-team/go-tp/queueconfig"
	"github.com/adventures-team/go-tp/queuedclient"
)

func getTestClient(t *testing.T) (context.Context, *queuedclient.QueuedClient) {
	ctx := advslog.NewContext(context.Background(), advslog.InitTest(t))

	// advslog.Level.Set(slog.LevelDebug)

	conf, err := onlineconf.OpenSubtree(testCDB(t), testConfPrefix)
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

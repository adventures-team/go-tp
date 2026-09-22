//go:build integration

package tptest

import (
	"context"
	"testing"

	advslog "github.com/adventures-team/go-adv-slog"

	// TODO: switch to the standard library uuid package once Go 1.27 reaches
	// a stable patch release (1.27.1/1.27.2), then drop github.com/gofrs/uuid.
	"github.com/gofrs/uuid"

	"github.com/adventures-team/go-queuedproto"

	"gitlab.adv.team/go/tp.git"
	"gitlab.adv.team/go/tp.git/queueconfig"
)

func TestNoHandler(t *testing.T) {
	// по сути, тест просто на то, что Register/Publish нормально переваривают NoHandler. логика не тестируется ввиду её отсутствия :))
	logger := advslog.InitTest(t)
	ctx := advslog.NewContext(context.Background(), logger)

	conf := newMockConf(t, testNoHandlerQueueID)
	queueConf := queueconfig.New(conf, testInstanceName)

	queue, err := tp.NewQueue(ctx, queueConf)
	if err != nil {
		t.Fatal("error initializing queue processor:", err)
	}

	uid, err := uuid.NewV7()
	if err != nil {
		t.Fatal("uuid.NewV7:", err)
	}

	eventID, err := queue.Publish(ctx, &eventPublishOnly{TransientID: 132123123, UUID: UUID{uid}})
	if err != nil {
		t.Fatal("Publish:", err)
	}

	logger.Info("Publish OK, deleting event", "event_id", eventID)

	if err = queue.Client().Delete(ctx, testNoHandlerQueueID, []queuedproto.EventID{eventID}); err != nil {
		t.Fatal("Delete:", err)
	}
}

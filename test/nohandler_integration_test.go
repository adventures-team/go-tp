//go:build integration

package tptest

import (
	"context"
	"testing"

	// TODO: switch to the standard library uuid package once Go 1.27 reaches
	// a stable patch release (1.27.1/1.27.2), then drop github.com/gofrs/uuid.
	"github.com/gofrs/uuid"

	"github.com/adventures-team/go-queuedproto"

	"gitlab.adv.team/go/tp.git"
	"gitlab.adv.team/go/tp.git/queueconfig"
	"gitlab.adv.team/go/utils.git/mylog"
)

func TestNoHandler(t *testing.T) {
	// по сути, тест просто на то, что Register/Publish нормально переваривают NoHandler. логика не тестируется ввиду её отсутствия :))
	logger := mylog.InitTestZerolog(t)
	ctx := logger.WithContext(context.Background())

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

	logger.Info().Stringer("event_id", eventID).Msg("Publish OK, deleting event")

	if err = queue.Client().Delete(ctx, testNoHandlerQueueID, []queuedproto.EventID{eventID}); err != nil {
		t.Fatal("Delete:", err)
	}
}

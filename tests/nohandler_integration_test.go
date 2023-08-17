//go:build integration
// +build integration

package tptest

import (
	"context"
	"testing"

	"github.com/gofrs/uuid"

	"gitlab.corp.mail.ru/mygo/queuedproto.git"
	"gitlab.corp.mail.ru/mygo/tp.git"
	"gitlab.corp.mail.ru/mygo/tp.git/queueconfig"
	"gitlab.corp.mail.ru/mygo/utils.git/mylog"
)

func TestNoHandler(t *testing.T) {
	// по сути, тест просто на то, что Register/Publish нормально переваривают NoHandler. логика не тестируется ввиду её отсутствия :))
	logger := mylog.InitTestZerolog(t)
	ctx := logger.WithContext(context.Background())

	conf := newMockConf(t, testNoHandlerQueueID)
	queueConf := queueconfig.New(conf, testInstanceName)
	queue, err := tp.Init(ctx, queueConf)

	if err != nil {
		t.Fatal("error initializing queue processor:", err)
	}

	if err = queue.Register(testNoHandlerQueueID, &eventPublishOnly{}, nil); err != nil {
		t.Fatal("Register:", err)
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

	if err = queue.Queued().Delete(ctx, testNoHandlerQueueID, []queuedproto.EventID{eventID}); err != nil {
		t.Fatal("Delete:", err)
	}
}

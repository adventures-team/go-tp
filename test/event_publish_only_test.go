//go:build integration
// +build integration

package tptest

import (
	"github.com/gofrs/uuid"

	"gitlab.cebc.tools/go/easygo.git/iproto"
	"gitlab.cebc.tools/go/tp.git"
)

// UUID - обёртка для передачи UUID при помощи easygo.
type UUID struct {
	uuid.UUID
}

// IprotoPack передаёт 16 байт UUID, как есть. Перловый формат: C16.
func (u UUID) IprotoPack(payload []byte, _ iproto.PackMode) ([]byte, error) {
	return append(payload, u.UUID[:]...), nil
}

// eventPublishOnly - без обработчика - публикуем и удаляем вручную
type eventPublishOnly struct {
	tp.BaseEvent

	TransientID uint64
	UUID     UUID
}

var _ tp.Event = &eventPublishOnly{}

func (eventPublishOnly) Version() uint8 {
	return 1
}

func (eventPublishOnly) QueueID() uint16 {
	return testNoHandlerQueueID
}

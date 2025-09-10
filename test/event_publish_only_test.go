//go:build integration

package tptest

import (
	"errors"

	"github.com/gofrs/uuid"

	"gitlab.cebc.tools/go/tp.git"
)

// UUID - uuid.UUID iproto wrapper
type UUID struct {
	uuid.UUID
}

// MarshalIProto передаёт 16 байт UUID, как есть. Перловый формат: C16.
func (u UUID) MarshalIProto(buf []byte) ([]byte, error) {
	return append(buf, u.UUID[:]...), nil
}

func (*UUID) UnmarshalIProto([]byte) ([]byte, error) {
	return nil, errors.New("UUID.UnmarshalIProto isn't implemented")
}

// eventPublishOnly - без обработчика - публикуем и удаляем вручную
//
//adv:iproto:
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

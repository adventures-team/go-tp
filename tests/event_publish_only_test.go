//go:build integration
// +build integration

package tptest

import (
	"gitlab.corp.mail.ru/mygo/tp.git"
)

// eventPublishOnly - без обработчика - публикуем и удаляем вручную
type eventPublishOnly struct {
	tp.NoHandler

	TransientID uint64
}

var _ tp.Handler = &eventPublishOnly{}

func (eventPublishOnly) Version() uint8 {
	return 1
}

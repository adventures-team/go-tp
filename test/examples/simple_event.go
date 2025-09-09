package main

import (
	"gitlab.cebc.tools/go/tp.git"
)

const SimpleQueueID = 1122

//adv:iproto:
type SimpleEvent struct {
	tp.BaseEvent

	UserID   uint32
	UserName string `iproto:"ber"`
}

func (SimpleEvent) Version() uint8 {
	return 1
}

func (SimpleEvent) QueueID() uint16 {
	return SimpleQueueID
}

type Service struct {
	DatabaseConnection any
}

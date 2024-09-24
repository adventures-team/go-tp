package main

import (
	"gitlab.corp.mail.ru/mygo/tp.git"
)

const SimpleQueueID = 1122

type SimpleEvent struct {
	tp.BaseEvent

	UserID   uint32
	UserName string
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

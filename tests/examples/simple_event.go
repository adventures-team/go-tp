package main

import "gitlab.corp.mail.ru/mygo/tp.git"

const SimpleQueueID = 1122

type SimpleEvent struct {
	tp.Event

	ID   uint32
	Name string
}

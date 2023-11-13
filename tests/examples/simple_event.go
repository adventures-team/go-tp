package main

import (
	"gitlab.corp.mail.ru/mygo/tp.git"
)

const SimpleQueueID = 1122

type SimpleEvent struct {
	tp.Event

	ID   uint32
	Name string

	Service *Service `iproto:"-"` // поля с этим тэгом игнорируются при сериализации
}

type Service struct {
	tp.DisableMarshaler
	DatabaseConnection any
}

package main

import (
	"context"

	zLog "github.com/rs/zerolog/log"
	"gitlab.corp.mail.ru/mygo/tp.git"
)

func sendData(id uint32, name string) {
	queue := tp.Config{
		Config: getConfig(),
	}.InitProducer()

	if err := queue.Register(SimpleQueueID, &SimpleEvent{}, nil); err != nil {
		zLog.Fatal().Err(err).Msg("error registering queue")
	}

	eventID, err := queue.Publish(context.Background(), &SimpleEvent{ID: id, Name: name})
	if err != nil {
		zLog.Fatal().Err(err).Msg("error publishing an event")
	}

	zLog.Info().Stringer("event_id", eventID).Msg("event published")
}

package main

import (
	"context"

	zLog "github.com/rs/zerolog/log"

	"gitlab.cebc.tools/go/tp.git"
)

func sendData(id uint32, name string) {
	queue := tp.Config{
		Config: getConfig(),
	}.InitProducer()

	eventID, err := queue.Publish(context.Background(), &SimpleEvent{UserID: id, UserName: name})
	if err != nil {
		zLog.Fatal().Err(err).Msg("error publishing an event")
	}

	zLog.Info().Stringer("event_id", eventID).Msg("event published")
}

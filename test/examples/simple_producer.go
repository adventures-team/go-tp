package main

import (
	"context"

	zlog "github.com/rs/zerolog/log"

	"gitlab.adv.team/go/tp.git"
)

func sendData(id uint32, name string) {
	queue := tp.InitProducer(getConfig())

	eventID, err := queue.Publish(context.Background(), &SimpleEvent{UserID: id, UserName: name})
	if err != nil {
		zlog.Fatal().Err(err).Msg("error publishing an event")
	}

	zlog.Info().Stringer("event_id", eventID).Msg("event published")
}

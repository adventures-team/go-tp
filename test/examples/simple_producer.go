package main

import (
	"context"
	"log/slog"

	advslog "github.com/adventures-team/go-adv-slog"

	"gitlab.adv.team/go/tp.git"
)

func sendData(id uint32, name string) {
	queue := tp.InitProducer(getConfig())

	eventID, err := queue.Publish(context.Background(), &SimpleEvent{UserID: id, UserName: name})
	if err != nil {
		advslog.Fatal("error publishing an event", advslog.Err(err))
	}

	slog.Info("event published", "event_id", eventID)
}

package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	zLog "github.com/rs/zerolog/log"

	"gitlab.corp.mail.ru/mygo/tp.git"
)

func (*SimpleEvent) Version() uint8 {
	return 1
}

func (ev *SimpleEvent) Handle(ctx context.Context) error {
	zerolog.Ctx(ctx).Info().Uint32("id", ev.ID).Str("name", ev.Name).Interface("service", ev.Service).Msg("SimpleEvent received")
	ev.Done()

	return nil
}

func startHandler() {
	qm := tp.Config{
		Config:       getConfig(),
		InstanceName: "test-instance",
	}.InitProcessor()

	service := &Service{DatabaseConnection: "test connection"}

	if err := qm.Register(SimpleQueueID, &SimpleEvent{}, service); err != nil {
		zLog.Fatal().Err(err).Msg("error registering queue")
	}

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGHUP)

	go func() {
		sig := <-sigc
		zLog.Info().Stringer("signal", sig).Msg("exiting on signal")
		qm.Stop()
	}()

	qm.Run()
}

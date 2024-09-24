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

func (svc *Service) Handle(ctx context.Context, events []SimpleEvent) error {
	for _, ev := range events {
		zerolog.Ctx(ctx).Info().Uint32("user_id", ev.UserID).Str("user_name", ev.UserName).Msg("SimpleEvent received")
		ev.Done()
	}

	return nil
}

func startHandler(svc *Service) {
	qm := tp.Config{
		Config:       getConfig(),
		InstanceName: "test-instance",
	}.InitProcessor()

	if err := tp.Register(qm.Queue, svc.Handle); err != nil {
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

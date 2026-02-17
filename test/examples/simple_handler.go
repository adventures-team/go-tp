package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"

	"gitlab.cebc.tools/go/tp.git"
)

func (*Service) Handle(ctx context.Context, events []SimpleEvent) error {
	for _, ev := range events {
		zerolog.Ctx(ctx).Info().Uint32("user_id", ev.UserID).Str("user_name", ev.UserName).Msg("SimpleEvent received")
		ev.Done()

		if !ev.Queue().Client().IsOnline() {
			return errors.New("queued is offline")
		}
	}

	return nil
}

func startHandler(svc *Service) {
	qm := tp.InitProcessor(getConfig(), tp.WithInstanceName("test-instance"))

	if err := tp.Register(qm.Queue, svc.Handle); err != nil {
		zlog.Fatal().Err(err).Msg("error registering queue")
	}

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGHUP)

	go func() {
		sig := <-sigc
		zlog.Info().Stringer("signal", sig).Msg("exiting on signal")
		qm.Stop()
	}()

	qm.Run()
}

package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	advslog "github.com/adventures-team/go-adv-slog"

	"gitlab.adv.team/go/tp.git"
)

func (*Service) Handle(ctx context.Context, events []SimpleEvent) error {
	for _, ev := range events {
		advslog.Ctx(ctx).Info("SimpleEvent received", "user_id", ev.UserID, "user_name", ev.UserName)
		ev.Done()

		if !ev.Queue().Client().IsHealthy() {
			return errors.New("queued is offline")
		}
	}

	return nil
}

func startHandler(svc *Service) {
	qm := tp.InitProcessor(getConfig(), tp.WithInstanceName("test-instance"))

	if err := tp.Register(qm.Queue, svc.Handle); err != nil {
		advslog.Fatal("error registering queue", advslog.Err(err))
	}

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGHUP)

	go func() {
		sig := <-sigc
		slog.Info("exiting on signal", "signal", sig.String())
		qm.Stop()
	}()

	qm.Run()
}

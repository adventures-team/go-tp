package main

import (
	"context"
	"log"
)

type (
	dumbLogger  struct{}
	debugLogger struct{ dumbLogger }
)

func (dumbLogger) Printf(_ context.Context, format string, v ...any) {
	log.Printf(format, v...)
}

func (dumbLogger) Debugf(_ context.Context, _ string, _ ...any) {
}

func (debugLogger) Debugf(_ context.Context, format string, v ...any) {
	log.Printf("[debug] "+format, v...)
}

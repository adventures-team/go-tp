// Package iprotolog адаптирует slog к интерфейсу Logger легаси-библиотеки easygo/iproto.
//
// Временный пакет: удаляется вместе с easygo при переходе на go-iprotoconn
// (временный адаптер на период перехода транспорта на go-iprotoconn).
package iprotolog

import (
	"context"
	"fmt"
	"log/slog"

	advslog "github.com/adventures-team/go-adv-slog"
)

// Logger реализует интерфейс easygo/iproto.Logger (структурно, без импорта easygo)
// поверх логгера из контекста ([advslog.Ctx]).
type Logger struct{}

func (Logger) Printf(ctx context.Context, format string, v ...any) {
	l := advslog.Ctx(ctx)
	if l.Enabled(ctx, slog.LevelInfo) {
		l.Info("iproto: " + fmt.Sprintf(format, v...))
	}
}

func (Logger) Debugf(ctx context.Context, format string, v ...any) {
	l := advslog.Ctx(ctx)
	if l.Enabled(ctx, slog.LevelDebug) {
		l.Debug("iproto: " + fmt.Sprintf(format, v...))
	}
}

package queuedclient

import (
	"context"

	"github.com/rs/zerolog"

	"gitlab.corp.mail.ru/e-mail-ru/easygo/iproto"
)

// IprotoLogger - адаптер из iproto.Logger в zerolog
type IprotoLogger struct{}

var _ iproto.Logger = IprotoLogger{}

func (IprotoLogger) Printf(ctx context.Context, fmt string, v ...interface{}) {
	zerolog.Ctx(ctx).Info().Msgf("iproto: "+fmt, v...)
}

func (IprotoLogger) Debugf(ctx context.Context, fmt string, v ...interface{}) {
	zerolog.Ctx(ctx).Debug().Msgf("iproto: "+fmt, v...)
}

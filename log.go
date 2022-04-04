package tp

import (
	"context"

	"github.com/rs/zerolog"
	zLog "github.com/rs/zerolog/log"
)

type baseLogCtxKey struct{}

// TODO стандартные контексты - немного не про производительность - а про копирование вместо модификации для избежания
// локов при мультитредовом доступе. Тут мультитредовых модификаций не будет by design, поэтому можно будет
// сделать свой контекст с хранением значений в sync.Map (или даже обычном map[interface{}]interface{}),
// + базовый логгер держать вообще в отдельном поле. Очереди про это знать не будут (будут принимать стандратный
// контекст), все type assertions будут в коде tp.
//
// Такое можем позволить, т.к. в обработчиках мы сами создаём контекст, и передаём его юзеру.
//
// Не делать это, пока не будут процесситься пачки по 1к событий!

func withBaseLog(ctx context.Context, logger *zerolog.Logger) context.Context {
	return context.WithValue(logger.WithContext(ctx), baseLogCtxKey{}, logger)
}

func logFromCtx(ctx context.Context) *zerolog.Logger {
	if li := ctx.Value(baseLogCtxKey{}); li != nil {
		return li.(*zerolog.Logger)
	}

	zLog.Error().Msg("intenal error - logFromCtx called with wrong context, using global logger")

	return &zLog.Logger
}

func nextEventLogger(pctx *context.Context, eventID uint64) *zerolog.Logger {
	logger := logFromCtx(*pctx).With().Uint64("event_id", eventID).Logger()
	*pctx = logger.WithContext(*pctx)

	return &logger
}

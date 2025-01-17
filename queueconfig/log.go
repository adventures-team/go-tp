package queueconfig

import (
	"context"

	"gitlab.corp.my.games/adventures/go/utils.git/mylog"
)

// ClearErrCache очищает кеш ошибочных путей. Вызывать при перезагрузке конфига.
func (c *Config) ClearErrCache() {
	c.pathErrors.Range(func(key, _ interface{}) bool { // операция редкая - поэтому так будет быстрее, чем делать локи/атомики на каждое обращение
		c.pathErrors.Delete(key)
		return true
	})
}

// если dfl указано (не nil), то ошибки ErrNotFound не логгируются
func (c *Config) logError(ctx context.Context, err error, dfl interface{}, msg string) {
	if cErr, ok := err.(*ConfError); ok {
		if dfl != nil {
			if cErr.isNotFound {
				return
			}

			cErr.Default = dfl
		}

		if _, ok := c.pathErrors.LoadOrStore(cErr.Path, nil); ok {
			return
		}
	}

	mylog.Ctx(ctx).Err(err).Msg(msg)
}

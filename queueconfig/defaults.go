package queueconfig

import (
	"time"

	advslog "github.com/adventures-team/go-adv-slog"
)

// Дефолты для некоторых значений. Используются, если значение не задано в конфиге нигде (ни для очереди, ни для инстанса, ни глобально).
const (
	DefaultLogLevel        = advslog.LevelTrace // default for log_level
	DefaultDownscaleRate   = 0.5                // default for oom_soft_downscale_rate
	DefaultOOMCheckPeriod  = 1 * time.Minute    // default for oom_check_period
	DefaultDeferTime       = 5 * time.Minute    // default for defer_time
	DefaultDeferTimeJitter = 1 * time.Minute    // default for defer_time_jitter
	DefaultInitialDelay    = time.Duration(0)   // default for initial_delay
	DefaultFetchBatchSize  = 10                 // default for fetch_batch_size
	DefaultUpdateBatchSize = 1000               // default for update_batch_size
)

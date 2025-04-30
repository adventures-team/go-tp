package queueconfig

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"time"

	"github.com/rs/zerolog"
)

// global/per instance

// OOMSoftLimit получает oom_soft_limit. По умолчанию - 0 (проверку нужно пропустить).
func (c *Config) OOMSoftLimit(ctx context.Context) uint64 {
	lim, err := c.getSizePerInstance("oom_soft_limit")
	if err != nil {
		c.logError(ctx, err, nil, "skipping soft memory limit check")
	}

	return lim
}

// OOMHardLimit получает oom_hard_limit. По умолчанию - 0 (проверку нужно пропустить).
func (c *Config) OOMHardLimit(ctx context.Context) uint64 {
	lim, err := c.getSizePerInstance("oom_hard_limit")
	if err != nil {
		c.logError(ctx, err, nil, "skipping hard memory limit check")
	}

	return lim
}

// OOMSoftDownscaleRate получает oom_soft_downscale_rate. По умолчанию: DefaultDownscaleRate.
func (c *Config) OOMSoftDownscaleRate(ctx context.Context) float64 {
	rate, err := c.getFloatPerInstance("oom_soft_downscale_rate")

	if rate < 0 {
		err = &ConfError{
			Path:    "oom_soft_downscale_rate",
			Value:   strconv.FormatFloat(rate, 'g', -1, 64),
			Err:     ErrOOMSoftDownscaleRate,
			Default: DefaultDownscaleRate,
		}
		rate = DefaultDownscaleRate
	} else if rate == 0 {
		rate = DefaultDownscaleRate
	}

	if err != nil {
		c.logError(ctx, err, DefaultDownscaleRate, "using the default")
	}

	return rate
}

// OOMCheckPeriod получает oom_check_period. По умолчанию: DefaultOOMCheckPeriod.
func (c *Config) OOMCheckPeriod(ctx context.Context) time.Duration {
	d, err := c.getDurationPerInstance("oom_check_period")
	if err == nil {
		return d
	}

	c.logError(ctx, err, DefaultOOMCheckPeriod, "using the default")

	return DefaultOOMCheckPeriod
}

// WorkersMaxCount получает workers_max
func (c *Config) WorkersMaxCount(ctx context.Context) (int, error) {
	count, err := c.getIntPerInstance("workers_max")
	if count < 0 {
		err = &ConfError{
			Path:  "workers_max",
			Value: strconv.Itoa(count),
			Err:   ErrWorkersCount,
		}
		count = 0
	}

	if err != nil {
		c.logError(ctx, err, nil, "workers_max is mandatory")
	}

	return count, err
}

// SlotsCount получает slots_count
func (c *Config) SlotsCount(ctx context.Context) (int, error) {
	count, err := c.getIntPerInstance("slots_count")
	if count < 0 {
		err = &ConfError{
			Path:  "slots_count",
			Value: strconv.Itoa(count),
			Err:   ErrSlotsCount,
		}
		count = 0
	}

	if err != nil {
		c.logError(ctx, err, nil, "slots_count is mandatory")
	}

	return count, err
}

// QueueIDs получает queue_ids, который задаётся в виде разделённых через запятую интов (см. CSVRegex).
// Если задан пустой список (пустая строка), ошибкой это не считается (требуется холостой запуск, либо остановка всех запущенных очередей)
func (c *Config) QueueIDs() ([]uint16, error) {
	str, err := c.getStringPerInstance("queue_ids")
	if err != nil {
		return nil, err
	}

	strIDs := CSVRegex.Split(str, -1)
	queueIDs := make([]uint16, 0, len(strIDs))

	for _, strID := range strIDs {
		if strID == "" {
			continue
		}

		id, err := strconv.ParseUint(strID, 10, 16)
		if err != nil {
			return nil, &ConfError{
				Path:  "queue_ids",
				Value: strID,
				Err:   err,
			}
		}

		queueIDs = append(queueIDs, uint16(id))
	}

	return queueIDs, nil // пустой список - не ошибка (значит надо остановить все запущенные очереди)
}

// LogLevel получает log_level. По умолчанию: DefaultLogLevel.
func (c *Config) LogLevel(ctx context.Context) zerolog.Level {
	str, err := c.getStringPerInstance("log_level")
	if err != nil {
		c.logError(ctx, err, DefaultLogLevel, "using the default")

		return DefaultLogLevel
	}

	level, err := zerolog.ParseLevel(str)
	if err != nil {
		level = DefaultLogLevel
		err = &ConfError{
			Path:    "log_level",
			Value:   str,
			Err:     err,
			Default: DefaultLogLevel,
		}

		c.logError(ctx, err, nil, "using the default")
	}

	return level
}

// per queue

// WorkersCount получает workers для указанной очереди, но не больше workers_max_per_queue
func (c *Config) WorkersCount(ctx context.Context, queueID uint16) (int, error) {
	count, err := c.getIntPerQueue(queueID, "workers")
	if err != nil {
		c.logError(ctx, err, nil, "workers parameter is mandatory")
		return 0, err
	}

	maxCount, err := c.getIntPerInstance("workers_max_per_queue")
	if err != nil {
		c.logError(ctx, err, -1, "ignoring invalid workers_max_per_queue")
		return count, nil
	}

	if maxCount < 0 {
		return count, nil
	}

	if count > maxCount {
		return maxCount, nil
	}

	return count, nil
}

// DeferTime получает defer_time для указанной очереди. Dалидное значение возвращается всегда (default: DefaultDeferTime), ошибку надо просто напечатать в лог.
// К времени добавляется рандомное значение, максимум defer_time_jitter (default: DefaultDeferTimeJitter)
func (c *Config) DeferTime(ctx context.Context, queueID uint16) time.Duration {
	deferTime, err := c.getDurationPerQueue(queueID, "defer_time")
	if err != nil {
		deferTime = DefaultDeferTime
		c.logError(ctx, err, DefaultDeferTime, "using the default")
	}

	jitter, err := c.getDurationPerQueue(queueID, "defer_time_jitter")
	if err != nil {
		jitter = DefaultDeferTimeJitter
		c.logError(ctx, err, DefaultDeferTimeJitter, "using the default")
	}

	return deferTime + time.Duration(rand.Int63n(int64(jitter))) //nolint:gosec
}

// InitialDelay получает initial_delay для указанной очереди. По умолчанию: DefaultInitialDelay.
func (c *Config) InitialDelay(ctx context.Context, queueID uint16) time.Duration {
	d, err := c.getDurationPerQueue(queueID, "initial_delay")
	if err == nil {
		return d
	}

	c.logError(ctx, err, DefaultInitialDelay, "using the default")

	return DefaultInitialDelay
}

// FetchBatchSize получает fetch_batch_size для указанной очереди. Если оно не задано, возвращается DefaultFetchBatchSize
func (c *Config) FetchBatchSize(ctx context.Context, queueID uint16) int {
	bsize, err := c.getIntPerQueue(queueID, "fetch_batch_size")
	if err == nil {
		return bsize
	}

	c.logError(ctx, err, DefaultFetchBatchSize, "using the default")

	return DefaultFetchBatchSize
}

// UpdateBatchSize возвращает update_batch_size для указанной очереди. Если оно не задано, возвращается DefaultUpdateBatchSize
func (c *Config) UpdateBatchSize(ctx context.Context, queueID uint16) int {
	bsize, err := c.getIntPerQueue(queueID, "update_batch_size")
	if err == nil {
		return bsize
	}

	c.logError(ctx, err, DefaultUpdateBatchSize, "using the default")

	return DefaultUpdateBatchSize
}

// MaxAge возвращает max_age для указанной очереди, или глобально заданное значение. Используется для middleware [tp.WithMaxAge]
func (c *Config) MaxAge(queueID uint16) (uint32, error) {
	d, err := c.getDurationPerQueue(queueID, "max_age")
	if err != nil {
		return 0, err
	}

	return uint32(d.Seconds()), nil
}

// retry_types
const (
	RetryConstant = iota
	RetryLinear
	RetryExponential
)

// RetryType возвращает тип задержки повтора retry_type, заданный для указанной очереди, или глобально.
// Используется для [tp.WithRetry] и [tp.WithRetry2].
// По умолчанию, возвращается RetryConstant.
func (c *Config) RetryType(queueID uint16) (int, error) {
	typ, path, err := c.getStringPerQueue(queueID, "retry_type")

	if err != nil {
		return RetryConstant, nil
	}

	switch typ {
	case "const", "constant":
		return RetryConstant, nil
	case "lin", "linear":
		return RetryLinear, nil
	case "exp", "exponential":
		return RetryExponential, nil
	}

	return 0, &ConfError{
		Path:  path,
		Value: typ,
		Err:   errors.New("unknown retry_type"),
	}
}

// RetryLimit возвращает лимит повторных попыток retry_limit для [tp.WithRetry] и [tp.WithRetry2]
func (c *Config) RetryLimit(queueID uint16, retrySuffix string) (uint32, error) {
	i, err := c.getIntPerQueue(queueID, "retry"+retrySuffix+"_limit")
	return uint32(i), err
}

// RetryDelay возвращает задержку retry_delay для [tp.WithRetry] и [tp.WithRetry2]
func (c *Config) RetryDelay(queueID uint16, retrySuffix string) (time.Duration, error) {
	return c.getDurationPerQueue(queueID, "retry"+retrySuffix+"_delay")
}

// RetryCoeff возвращает коэффициент retry_coeff для [tp.WithRetry] и [tp.WithRetry2] в режимах RetryLinear и RetryExponential
func (c *Config) RetryCoeff(queueID uint16, retrySuffix string) (time.Duration, error) {
	return c.getDurationPerQueue(queueID, "retry"+retrySuffix+"_coeff")
}

// RetryPower возвращает экспоненту retry_exp для [tp.WithRetry] и [tp.WithRetry2] в режиме RetryExponential.
// По умолчанию, возвращается значение 2.
func (c *Config) RetryExp(queueID uint16, retrySuffix string) (float64, error) {
	pow, err := c.getFloatPerQueue(queueID, "retry"+retrySuffix+"_exp")
	if err != nil {
		if confErr, ok := err.(*ConfError); ok && confErr.isNotFound {
			return 2, nil
		}
	}

	return pow, err
}

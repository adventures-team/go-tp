package queueconfig

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/rs/zerolog"
)

// global/per instance

// OOMSoftLimit получает oom_soft_limit
func (c *Config) OOMSoftLimit() (uint64, error) {
	return c.getSizePerInstance("oom_soft_limit")
}

// OOMHardLimit получает oom_hard_limit
func (c *Config) OOMHardLimit() (uint64, error) {
	return c.getSizePerInstance("oom_hard_limit")
}

// OOMSoftDownscaleRate получает oom_soft_downscale_rate. Валидное значение возвращается всегда (default: DefaultDownscaleRate), ошибку надо просто напечатать в лог
func (c *Config) OOMSoftDownscaleRate() (float64, error) {
	rate, err := c.getFloatPerInstance("oom_soft_downscale_rate")

	if rate < 0 {
		err = fmt.Errorf("oom_soft_downscale_rate should be >= 0 (%g used)", rate)
		rate = DefaultDownscaleRate
	} else if rate == 0 {
		rate = DefaultDownscaleRate
	}

	return rate, err
}

// OOMCheckPeriod получает oom_check_period. Валидное значение возвращается всегда (default: DefaultOOMCheckPeriod), ошибку надо просто напечатать в лог
func (c *Config) OOMCheckPeriod() (time.Duration, error) {
	d, err := c.getDurationPerInstance("oom_check_period")
	if err == nil {
		return d, nil
	}

	if errors.Is(err, ErrNotFound) {
		return DefaultOOMCheckPeriod, nil
	}

	return DefaultOOMCheckPeriod, err
}

// WorkersMaxCount получает workers_max
func (c *Config) WorkersMaxCount() (int, bool) {
	count, ok := c.getIntPerInstance("workers_max")
	if !ok || count < 0 {
		return 0, false
	}

	return count, true
}

// QueueIDs получает queue_ids, который задаётся в виде разделённых через запятую интов (см. CSVRegex).
// Если задан пустой список (пустая строка), ошибкой это не считается (требуется холостой запуск, либо остановка всех запущенных очередей)
func (c *Config) QueueIDs() ([]uint16, error) {
	str, ok := c.getStringPerInstance("queue_ids")
	if !ok {
		return nil, fmt.Errorf("queue_ids: %w", ErrNotFound)
	}

	strIDs := CSVRegex.Split(str, -1)
	queueIDs := make([]uint16, 0, len(strIDs))

	for _, strID := range strIDs {
		if strID == "" {
			continue
		}

		id, err := strconv.ParseUint(strID, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("queue_ids: %s: %w", strID, err)
		}

		queueIDs = append(queueIDs, uint16(id))
	}

	return queueIDs, nil // пустой список - не ошибка (значит надо остановить все запущенные очереди)
}

// LogLevel получает log_level. Валидное значение возвращается всегда (default: DefaultLogLevel), ошибку надо просто напечатать в лог
func (c *Config) LogLevel() (zerolog.Level, error) {
	str, ok := c.getStringPerInstance("log_level")
	if !ok {
		return DefaultLogLevel, nil
	}

	level, err := zerolog.ParseLevel(str)
	if err != nil {
		return DefaultLogLevel, fmt.Errorf("log_level: %s: %w", str, err)
	}

	return level, nil
}

// per queue

// WorkersCount получает workers для указанной очереди, но не больше workers_max_per_queue
func (c *Config) WorkersCount(queueID uint16) (int, bool) {
	count, ok := c.getIntPerQueue(queueID, "workers")
	if !ok {
		return 0, false
	}

	maxCount, ok := c.getIntPerInstance("workers_max_per_queue")
	if !ok || maxCount <= 0 {
		return count, true
	}

	if count > maxCount {
		return maxCount, true
	}

	return count, true
}

// DeferTime получает defer_time для указанной очереди. Dалидное значение возвращается всегда (default: DefaultDeferTime), ошибку надо просто напечатать в лог.
// К времени добавляется рандомное значение, максимум defer_time_jitter (default: DefaultDeferTimeJitter)
func (c *Config) DeferTime(queueID uint16) (time.Duration, error) {
	deferTime, err := c.getDurationPerQueue(queueID, "defer_time")
	if err != nil {
		deferTime = DefaultDeferTime

		if errors.Is(err, ErrNotFound) {
			err = nil
		}
	}

	jitter, errj := c.getDurationPerQueue(queueID, "defer_time_jitter")
	if errj != nil {
		jitter = DefaultDeferTimeJitter

		if errors.Is(err, ErrNotFound) {
			errj = nil
		}
	}

	if err == nil {
		err = errj
	}

	return deferTime + time.Duration(rand.Intn(int(jitter))), err //nolint:gosec
}

// InitialDelay получает initial_delay для указанной очереди. Валидное значение возвращается всегда (default: DefaultInitialDelay), ошибку надо просто напечатать в лог
func (c *Config) InitialDelay(queueID uint16) (time.Duration, error) {
	d, err := c.getDurationPerQueue(queueID, "initial_delay")
	if err == nil {
		return d, nil
	}

	if errors.Is(err, ErrNotFound) {
		return DefaultInitialDelay, nil
	}

	return DefaultInitialDelay, err
}

// FetchBatchSize получает fetch_batch_size для указанной очереди. Если оно не задано, возвращается DefaultFetchBatchSize
func (c *Config) FetchBatchSize(queueID uint16) int {
	if bsize, ok := c.getIntPerQueue(queueID, "fetch_batch_size"); ok {
		return bsize
	}

	return DefaultFetchBatchSize
}

// UpdateBatchSize возвращает update_batch_size для указанной очереди. Если оно не задано, возвращается DefaultUpdateBatchSize
func (c *Config) UpdateBatchSize(queueID uint16) int {
	if bsize, ok := c.getIntPerQueue(queueID, "update_batch_size"); ok {
		return bsize
	}

	return DefaultUpdateBatchSize
}

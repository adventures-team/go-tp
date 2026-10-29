package mover

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	advslog "github.com/adventures-team/go-adv-slog"
	"github.com/onlineconf/onlineconf-go/v2"

	"github.com/adventures-team/go-tp/queueconfig"
)

// Conf - поддерево onlineconf с настройками утилиты: нужная утилите часть интерфейса
// [onlineconf.Subtree]. Пути параметров - относительно этого поддерева.
type Conf interface {
	GetStringIfExists(path string) (string, bool)
	GetIntErr(path string) (int, error)
	GetBool(path string, dfl bool) bool
	GetDurationErr(path string) (time.Duration, error)
	SubscribeSubtree(path string) (chan struct{}, error)
}

var _ Conf = (*onlineconf.Subtree)(nil)

// Пути параметров утилиты относительно её поддерева onlineconf (описание - godoc cmd/queued-mover).
const (
	ConfQueueIDs       = "/queue_ids"        // очереди для переноса, формат /tp/go/queue_ids
	ConfOneShot        = "/one_shot"         // выйти, когда все очереди пусты
	ConfPreserveIDs    = "/preserve_ids"     // сохранять ID событий
	ConfWorkers        = "/workers"          // воркеров на очередь
	ConfFetchBatchSize = "/fetch_batch_size" // событий за один GetActive с каждого шарда
	ConfInitialDelay   = "/initial_delay"    // задержка активации перенесённого события в B
	ConfDeferTime      = "/defer_time"       // на сколько откладывать в A неперенесённое событие
	ConfPollDelay      = "/poll_delay"       // пауза после пустой выборки и после пачки с ошибками
	ConfStatPeriod     = "/stat_period"      // период строки прогресса и перечитывания queue_ids/workers
	ConfLogLevel       = "/log_level"        // уровень логгирования
)

// Значения по умолчанию.
const (
	DefaultWorkers        = 1
	DefaultFetchBatchSize = 100
	DefaultInitialDelay   = time.Duration(0)
	DefaultDeferTime      = time.Minute
	DefaultPollDelay      = time.Second
	DefaultStatPeriod     = time.Minute
	DefaultLogLevel       = slog.LevelInfo
)

// ErrNegative - длительность не может быть отрицательной.
var ErrNegative = errors.New("value must not be negative")

// settings - доступ к параметрам утилиты. Значения читаются из onlineconf при каждом обращении
// (как в queueconfig): изменения действуют без перезапуска. Об ошибочном значении в лог пишется
// один раз (до [settings.clearErrCache]), используется значение по умолчанию.
type settings struct {
	conf   Conf
	logged sync.Map // пути, про которые уже было сообщение в лог
}

func newSettings(conf Conf) *settings {
	return &settings{conf: conf}
}

// validate проверяет параметры при старте: обязательный queue_ids и синтаксис остальных.
// Возвращает первую найденную ошибку.
func (s *settings) validate() error {
	if _, err := s.queueIDs(); err != nil {
		return err
	}

	for _, path := range []string{ConfWorkers, ConfFetchBatchSize} {
		if _, err := s.conf.GetIntErr(path); err != nil && !errors.Is(err, onlineconf.ErrNotFound) {
			return s.confError(path, err, nil)
		}
	}

	for _, path := range []string{ConfInitialDelay, ConfDeferTime, ConfPollDelay, ConfStatPeriod} {
		d, err := s.conf.GetDurationErr(path)
		if err != nil && !errors.Is(err, onlineconf.ErrNotFound) {
			return s.confError(path, err, nil)
		}

		if d < 0 {
			return s.confError(path, ErrNegative, nil)
		}
	}

	if str, ok := s.conf.GetStringIfExists(ConfLogLevel); ok {
		if _, err := advslog.ParseLevel(str); err != nil {
			return s.confError(ConfLogLevel, err, nil)
		}
	}

	return nil
}

// clearErrCache очищает список путей, про ошибки в которых уже сообщалось. Вызывать при перечитывании конфига.
func (s *settings) clearErrCache() {
	s.logged.Range(func(key, _ any) bool {
		s.logged.Delete(key)
		return true
	})
}

// queueIDs - обязательный список очередей (пустой список допустим).
func (s *settings) queueIDs() ([]uint16, error) {
	str, ok := s.conf.GetStringIfExists(ConfQueueIDs)
	if !ok {
		return nil, fmt.Errorf("%s: %w", ConfQueueIDs, queueconfig.ErrNotFound)
	}

	return queueconfig.ParseQueueIDs(str)
}

func (s *settings) oneShot() bool {
	return s.conf.GetBool(ConfOneShot, false)
}

func (s *settings) preserveIDs() bool {
	return s.conf.GetBool(ConfPreserveIDs, false)
}

func (s *settings) workers(ctx context.Context) int {
	return s.intSetting(ctx, ConfWorkers, DefaultWorkers, 0)
}

func (s *settings) fetchBatchSize(ctx context.Context) int {
	return s.intSetting(ctx, ConfFetchBatchSize, DefaultFetchBatchSize, 1)
}

func (s *settings) initialDelay(ctx context.Context) time.Duration {
	return s.durationSetting(ctx, ConfInitialDelay, DefaultInitialDelay)
}

func (s *settings) deferTime(ctx context.Context) time.Duration {
	return s.durationSetting(ctx, ConfDeferTime, DefaultDeferTime)
}

func (s *settings) pollDelay(ctx context.Context) time.Duration {
	return s.durationSetting(ctx, ConfPollDelay, DefaultPollDelay)
}

func (s *settings) statPeriod(ctx context.Context) time.Duration {
	return s.durationSetting(ctx, ConfStatPeriod, DefaultStatPeriod)
}

func (s *settings) logLevel(ctx context.Context) slog.Level {
	str, ok := s.conf.GetStringIfExists(ConfLogLevel)
	if !ok {
		return DefaultLogLevel
	}

	level, err := advslog.ParseLevel(str)
	if err != nil {
		s.logOnce(ctx, ConfLogLevel, err, DefaultLogLevel)

		return DefaultLogLevel
	}

	return level
}

// intSetting читает целое значение не меньше minimum; не задано - dfl, ошибка - лог и dfl.
func (s *settings) intSetting(ctx context.Context, path string, dfl, minimum int) int {
	value, err := s.conf.GetIntErr(path)
	if errors.Is(err, onlineconf.ErrNotFound) {
		return dfl
	}

	if err == nil && value < minimum {
		err = fmt.Errorf("value must be >= %d", minimum)
	}

	if err != nil {
		s.logOnce(ctx, path, err, dfl)

		return dfl
	}

	return value
}

// durationSetting читает неотрицательную длительность (голое число - секунды, иначе синтаксис
// time.ParseDuration - как в onlineconf-go); не задано - dfl, ошибка - лог и dfl.
func (s *settings) durationSetting(ctx context.Context, path string, dfl time.Duration) time.Duration {
	value, err := s.conf.GetDurationErr(path)
	if errors.Is(err, onlineconf.ErrNotFound) {
		return dfl
	}

	if err == nil && value < 0 {
		err = ErrNegative
	}

	if err != nil {
		s.logOnce(ctx, path, err, dfl)

		return dfl
	}

	return value
}

// confError строит [queueconfig.ConfError] с текущим значением параметра.
func (s *settings) confError(path string, err error, dfl any) *queueconfig.ConfError {
	value, _ := s.conf.GetStringIfExists(path)

	return &queueconfig.ConfError{
		Path:    path,
		Value:   value,
		Err:     err,
		Default: dfl,
	}
}

// logOnce пишет об ошибочном значении параметра один раз на путь.
func (s *settings) logOnce(ctx context.Context, path string, err error, dfl any) {
	if _, seen := s.logged.LoadOrStore(path, struct{}{}); seen {
		return
	}

	advslog.Ctx(ctx).Error("invalid setting, using the default", advslog.Err(s.confError(path, err, dfl)))
}

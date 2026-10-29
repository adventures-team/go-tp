package mover

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	advslog "github.com/adventures-team/go-adv-slog"
	"github.com/google/go-cmp/cmp"
	"github.com/onlineconf/onlineconf-go/v2"

	"github.com/adventures-team/go-tp/queueconfig"
)

// fakeConf - Conf в памяти с семантикой onlineconf-go: GetBool ("" и "0" - false), GetDurationErr
// (голое число - секунды), уведомления подписчиков поддерева при изменении значений.
type fakeConf struct {
	mu     sync.Mutex
	values map[string]string
	subs   []chan struct{}
}

func newFakeConf(values map[string]string) *fakeConf {
	c := &fakeConf{values: make(map[string]string, len(values))}

	for path, value := range values {
		c.values[path] = value
	}

	return c
}

func (c *fakeConf) GetStringIfExists(path string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	value, ok := c.values[path]

	return value, ok
}

func (c *fakeConf) GetIntErr(path string) (int, error) {
	value, ok := c.GetStringIfExists(path)
	if !ok {
		return 0, onlineconf.ErrNotFound
	}

	return strconv.Atoi(value)
}

func (c *fakeConf) GetBool(path string, dfl bool) bool {
	value, ok := c.GetStringIfExists(path)
	if !ok {
		return dfl
	}

	return value != "" && value != "0"
}

func (c *fakeConf) GetDurationErr(path string) (time.Duration, error) {
	value, ok := c.GetStringIfExists(path)
	if !ok {
		return 0, onlineconf.ErrNotFound
	}

	if !strings.ContainsAny(value, "hms") {
		value += "s"
	}

	return time.ParseDuration(value)
}

func (c *fakeConf) SubscribeSubtree(string) (chan struct{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ch := make(chan struct{}, 1)
	c.subs = append(c.subs, ch)

	return ch, nil
}

// set меняет значение и уведомляет подписчиков (неблокирующе, как onlineconf-go).
func (c *fakeConf) set(path, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.values[path] = value

	for _, ch := range c.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func testCtx(t *testing.T) context.Context {
	t.Helper()

	return advslog.NewContext(context.Background(), advslog.InitTest(t))
}

func TestSettingsDefaults(t *testing.T) {
	ctx := testCtx(t)
	s := newSettings(newFakeConf(map[string]string{ConfQueueIDs: ""}))

	if err := s.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	ids, err := s.queueIDs()
	if err != nil || len(ids) != 0 {
		t.Errorf("queueIDs() = %v, %v; want empty list", ids, err)
	}

	if s.oneShot() || s.preserveIDs() {
		t.Errorf("oneShot() = %v, preserveIDs() = %v; want false", s.oneShot(), s.preserveIDs())
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"workers", s.workers(ctx), DefaultWorkers},
		{"fetch_batch_size", s.fetchBatchSize(ctx), DefaultFetchBatchSize},
		{"initial_delay", s.initialDelay(ctx), DefaultInitialDelay},
		{"defer_time", s.deferTime(ctx), DefaultDeferTime},
		{"poll_delay", s.pollDelay(ctx), DefaultPollDelay},
		{"stat_period", s.statPeriod(ctx), DefaultStatPeriod},
		{"log_level", s.logLevel(ctx), DefaultLogLevel},
	}

	for _, check := range checks {
		if check.got != check.want {
			t.Errorf("%s = %v, want default %v", check.name, check.got, check.want)
		}
	}
}

func TestSettingsValues(t *testing.T) {
	ctx := testCtx(t)
	s := newSettings(newFakeConf(map[string]string{
		ConfQueueIDs:       "1122, 1123",
		ConfOneShot:        "1",
		ConfPreserveIDs:    "yes",
		ConfWorkers:        "3",
		ConfFetchBatchSize: "25",
		ConfInitialDelay:   "1.5",
		ConfDeferTime:      "2m",
		ConfPollDelay:      "0.2",
		ConfStatPeriod:     "10s",
		ConfLogLevel:       "debug",
	}))

	if err := s.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	ids, err := s.queueIDs()
	if err != nil {
		t.Fatalf("queueIDs: %v", err)
	}

	if diff := cmp.Diff([]uint16{1122, 1123}, ids); diff != "" {
		t.Errorf("queueIDs mismatch (-want +got):\n%s", diff)
	}

	if !s.oneShot() || !s.preserveIDs() {
		t.Errorf("oneShot() = %v, preserveIDs() = %v; want true", s.oneShot(), s.preserveIDs())
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"workers", s.workers(ctx), 3},
		{"fetch_batch_size", s.fetchBatchSize(ctx), 25},
		{"initial_delay", s.initialDelay(ctx), 1500 * time.Millisecond},
		{"defer_time", s.deferTime(ctx), 2 * time.Minute},
		{"poll_delay", s.pollDelay(ctx), 200 * time.Millisecond},
		{"stat_period", s.statPeriod(ctx), 10 * time.Second},
		{"log_level", s.logLevel(ctx), slog.LevelDebug},
	}

	for _, check := range checks {
		if check.got != check.want {
			t.Errorf("%s = %v, want %v", check.name, check.got, check.want)
		}
	}
}

func TestSettingsValidateErrors(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		path   string // ConfError.Path ожидаемой ошибки; "" - ошибка queue_ids
	}{
		{name: "no queue_ids", values: map[string]string{}},
		{name: "bad queue_ids", values: map[string]string{ConfQueueIDs: "1,x"}, path: "queue_ids"},
		{name: "bad workers", values: map[string]string{ConfQueueIDs: "1", ConfWorkers: "two"}, path: ConfWorkers},
		{name: "bad fetch_batch_size", values: map[string]string{ConfQueueIDs: "1", ConfFetchBatchSize: ""}, path: ConfFetchBatchSize},
		{name: "bad duration", values: map[string]string{ConfQueueIDs: "1", ConfPollDelay: "soon"}, path: ConfPollDelay},
		{name: "negative duration", values: map[string]string{ConfQueueIDs: "1", ConfDeferTime: "-1m"}, path: ConfDeferTime},
		{name: "bad log_level", values: map[string]string{ConfQueueIDs: "1", ConfLogLevel: "loud"}, path: ConfLogLevel},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newSettings(newFakeConf(tt.values)).validate()
			if err == nil {
				t.Fatal("validate: no error")
			}

			if tt.path == "" {
				if !errors.Is(err, queueconfig.ErrNotFound) {
					t.Errorf("validate: %v, want ErrNotFound", err)
				}

				return
			}

			var confErr *queueconfig.ConfError

			if !errors.As(err, &confErr) || confErr.Path != tt.path {
				t.Errorf("validate: %v, want *ConfError with Path %s", err, tt.path)
			}
		})
	}
}

// Значения, допустимые синтаксически, но вне диапазона, при использовании заменяются
// значением по умолчанию (с единственной записью в лог).
func TestSettingsOutOfRange(t *testing.T) {
	ctx := testCtx(t)
	s := newSettings(newFakeConf(map[string]string{
		ConfQueueIDs:       "1",
		ConfWorkers:        "-1",
		ConfFetchBatchSize: "0",
		ConfLogLevel:       "loud",
	}))

	if got := s.workers(ctx); got != DefaultWorkers {
		t.Errorf("workers = %d, want default %d", got, DefaultWorkers)
	}

	if got := s.fetchBatchSize(ctx); got != DefaultFetchBatchSize {
		t.Errorf("fetchBatchSize = %d, want default %d", got, DefaultFetchBatchSize)
	}

	if got := s.logLevel(ctx); got != DefaultLogLevel {
		t.Errorf("logLevel = %v, want default %v", got, DefaultLogLevel)
	}

	logged := 0

	s.logged.Range(func(_, _ any) bool {
		logged++
		return true
	})

	if logged != 3 {
		t.Errorf("%d paths logged, want 3", logged)
	}

	s.clearErrCache()

	s.logged.Range(func(key, _ any) bool {
		t.Errorf("path %v is still in the error cache after clearErrCache", key)
		return true
	})
}

//go:build !integration

package tptest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	advmetricsset "github.com/my-mail-ru/go-adv-metrics/set"

	"gitlab.adv.team/go/tp.git"
	"gitlab.adv.team/go/tp.git/queueconfig"
	"gitlab.adv.team/go/tp.git/queuedclient"
	mockevent "gitlab.adv.team/go/tp.git/test/mock/event"
	mockonlineconf "gitlab.adv.team/go/tp.git/test/mock/onlineconf"
	mockqueue "gitlab.adv.team/go/tp.git/test/mock/queue"
)

func TestWithRetry(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	t.Run("success", func(t *testing.T) {
		ev := mockevent.NewMockRetryableEvent(ctrl)

		ev.EXPECT().NeedRetry().Return(false)

		err := tp.WithRetry(func(context.Context, []tp.RetryableEvent) error { return nil },
			tp.OnRetry(func(tp.RetryableEvent) { t.Fatal("OnRetry shouldn't be called") }),
			tp.OnRetryExhausted(func(tp.RetryableEvent) { t.Fatal("OnRetryExhausted shouldn't be called") }))(t.Context(), []tp.RetryableEvent{ev})

		require.NoError(t, err, "handler shouldn't return an error")
	})

	t.Run("retry (default defer)", func(t *testing.T) {
		ev := mockevent.NewMockRetryableEvent(ctrl)
		q := mockqueue.NewMockQueue(ctrl)

		ev.EXPECT().NeedRetry().Return(true)
		ev.EXPECT().IsDone().Return(false)
		ev.EXPECT().Queue().AnyTimes().Return(q)
		ev.EXPECT().QueueID().Return(12345)
		ev.EXPECT().GetRetryCount().Return(2)
		ev.EXPECT().IncRetryCount()
		ev.EXPECT().HasCustomDefer().Return(false)

		q.EXPECT().Config().Return(newTestWithRetryConf(ctrl))
		q.EXPECT().MetricsSet().AnyTimes().Return(advmetricsset.Set{})
		q.EXPECT().Replace(gomock.Any(), ev, gomock.Any()).DoAndReturn(func(_ context.Context, _ tp.Event, opts ...queuedclient.PublishOption) error {
			var gotOpt, wantOpt queuedclient.PublishOptions

			for _, f := range opts {
				f(&gotOpt)
			}

			queuedclient.PublishInitialDelay(5 * time.Second)(&wantOpt)

			if gotOpt != wantOpt {
				return errors.New("Replace: initial delay of 5 seconds wasn't set")
			}

			return nil
		})

		onRetry := 0

		err := tp.WithRetry(func(context.Context, []tp.RetryableEvent) error { return nil },
			tp.OnRetry(func(tp.RetryableEvent) { onRetry++ }),
			tp.OnRetryExhausted(func(tp.RetryableEvent) { t.Fatal("OnRetryExhausted shouldn't be called") }))(t.Context(), []tp.RetryableEvent{ev})

		require.NoError(t, err, "handler shouldn't return an error")
		require.Equal(t, onRetry, 1, "OnRetry should be called exactly once")
	})

	t.Run("retry (custom defer)", func(t *testing.T) {
		ev := mockevent.NewMockRetryableEvent(ctrl)
		q := mockqueue.NewMockQueue(ctrl)

		ev.EXPECT().NeedRetry().Return(true)
		ev.EXPECT().IsDone().Return(false)
		ev.EXPECT().Queue().AnyTimes().Return(q)
		ev.EXPECT().QueueID().Return(12345)
		ev.EXPECT().GetRetryCount().Return(2)
		ev.EXPECT().IncRetryCount()
		ev.EXPECT().HasCustomDefer().Return(true)

		q.EXPECT().Config().Return(newTestWithRetryConf(ctrl))
		q.EXPECT().MetricsSet().AnyTimes().Return(advmetricsset.Set{})
		q.EXPECT().Replace(gomock.Any(), ev).Return(nil)

		onRetry := 0

		err := tp.WithRetry(func(context.Context, []tp.RetryableEvent) error { return nil },
			tp.OnRetry(func(tp.RetryableEvent) { onRetry++ }),
			tp.OnRetryExhausted(func(tp.RetryableEvent) { t.Fatal("OnRetryExhausted shouldn't be called") }))(t.Context(), []tp.RetryableEvent{ev})

		require.NoError(t, err, "handler shouldn't return an error")
		require.Equal(t, onRetry, 1, "OnRetry should be called exactly once")
	})

	t.Run("exhausted", func(t *testing.T) {
		ev := mockevent.NewMockRetryableEvent(ctrl)
		q := mockqueue.NewMockQueue(ctrl)

		ev.EXPECT().NeedRetry().Return(true)
		ev.EXPECT().IsDone().Return(false)
		ev.EXPECT().Queue().AnyTimes().Return(q)
		ev.EXPECT().QueueID().Return(12345)
		ev.EXPECT().GetRetryCount().Return(3)
		ev.EXPECT().Done()

		q.EXPECT().Config().Return(newTestWithRetryConf(ctrl))
		q.EXPECT().MetricsSet().AnyTimes().Return(advmetricsset.Set{})

		exhausted := 0

		err := tp.WithRetry(func(context.Context, []tp.RetryableEvent) error { return nil },
			tp.OnRetry(func(tp.RetryableEvent) { t.Fatal("OnRetry shouldn't be called") }),
			tp.OnRetryExhausted(func(tp.RetryableEvent) { exhausted++ }))(t.Context(), []tp.RetryableEvent{ev})

		require.NoError(t, err, "handler shouldn't return an error")
		require.Equal(t, exhausted, 1, "OnRetryExhausted should be called exactly once")
	})
}

func newTestWithRetryConf(ctrl *gomock.Controller) *queueconfig.Config {
	oconf := mockonlineconf.NewMockOnlineConfWithSubtree(ctrl)
	iconf := mockonlineconf.NewMockOnlineConf(ctrl)

	oconf.EXPECT().Subtree("/tp/go").Return(nil) // should not be used, panic is ok
	oconf.EXPECT().Subtree("/tp/go/test").Return(iconf)
	iconf.EXPECT().GetStringIfExists("/12345/retry_type").Return("const", true)
	iconf.EXPECT().GetIntErr("/12345/retry_limit").Return(3, nil)
	iconf.EXPECT().GetStringIfExists("/12345/retry_delay").Return("5s", true)

	return queueconfig.New(oconf, "test")
}

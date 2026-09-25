package tp

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	advmetricsset "github.com/adventures-team/go-adv-metrics/set"
	"github.com/adventures-team/go-queuedproto/addons"

	"github.com/adventures-team/go-tp/queueconfig"
	"github.com/adventures-team/go-tp/queuedclient"
)

type Handler[EvType Event, EvSlice ~[]EvType] func(context.Context, EvSlice) error

type Middleware[EvType Event, EvSlice ~[]EvType] func(Handler[EvType, EvSlice]) Handler[EvType, EvSlice]

// EventWithCreationTime - событие, встраивающее [addons.CreationTime].
//
// Поддерживает middleware [addons.CreationTime]
type EventWithCreationTime interface {
	Event
	GetCreationTime() addons.CreationTime
}

// WithMaxAge - ограничивает время обработки события значением max_age, которое может задаваться в конфиге для очереди, инстанса или глобально.
//
// Пример:
//
//	tp.Register(q, tp.WithMaxAge(func (ctx context.Context, events []MyEvent) error {
//	    // обычный обработчик
//	    for _, event := range events {
//	        // события старше max_age сюда не попадут
//	    }
//	    return nil
//	}))
func WithMaxAge[EvType EventWithCreationTime, EvSlice ~[]EvType](handler Handler[EvType, EvSlice]) Handler[EvType, EvSlice] {
	return func(ctx context.Context, in EvSlice) error {
		events := make(EvSlice, 0, len(in))
		now := uint32(time.Now().Unix())

		maxAge, err := in[0].Queue().Config().MaxAge(in[0].QueueID())
		if err != nil {
			return err
		}

		minTime := now - maxAge

		for _, event := range in {
			if uint32(event.GetCreationTime()) < minTime {
				event.Done()

				if ms := event.Queue().MetricsSet(); (ms != advmetricsset.Set{}) {
					ms.Name("tp_events_expired").Int("queue_id", int(event.QueueID())).GetOrCreateCounter().Inc()
				}
			} else {
				events = append(events, event)
			}
		}

		return handler(ctx, events)
	}
}

// RetryableEvent - событие, встраивающее [addons.Retryable].
// Поддерживает middleware [WithRetry].
type RetryableEvent interface {
	Event
	IncRetryCount()
	GetRetryCount() uint32
	Retry()
	NeedRetry() bool
}

// RetryOption - [OnRetry], [OnRetryExhausted]
type RetryOption func(*retryOpt)

type retryOpt struct {
	onRetry          func(RetryableEvent)
	onRetryExhausted func(RetryableEvent)
}

// OnRetry определяет пользовательский обработчик, который будет вызван при ретрае.
// Используется для реализации кастомной обработки события при ретрае, будь то логгирование, отправка метрики и т.п.
func OnRetry[T RetryableEvent](f func(T)) RetryOption {
	return func(opt *retryOpt) {
		opt.onRetry = func(ev RetryableEvent) {
			f(ev.(T))
		}
	}
}

// OnRetryExhausted определяет пользовательский обработчик, который будет вызван при исчерпании лимита ретраев.
// Используется для реализации кастомной обработки события при ретрае, будь то логгирование, отправка метрики и т.п.
func OnRetryExhausted[T RetryableEvent](f func(T)) RetryOption {
	return func(opt *retryOpt) {
		opt.onRetryExhausted = func(ev RetryableEvent) {
			f(ev.(T))
		}
	}
}

// WithRetry - ограничивает количество попыток обработки события.
//
// При помощи опций можно задать коллбеки:
//   - [OnRetry] - вызывается после увеличения счётчика событий в случае неудачной обработки (вызова метода Retry). После последней попытки не вызывается.
//   - [OnRetryExhausted] - вызывается после последней неудачной попытки обработки.
//
// Настройки в onlineconf (задаются для очереди или глобально):
//
//	retry_limit - максимальное количество попыток, после которого событие удаляется. Формат - int
//	retry_type  - тип задержки - const или constant - постоянная, lin или linear - линейная, exp или exponential - экспоненциальная
//	retry_delay - постоянная часть задержки (для всех типов). Формат - [time.ParseDuration]
//	retry_coeff - коэффициент (для линейной или экспоненциальной задержки). Формат - [time.ParseDuration]
//	retry_exp   - экспонента. Формат - float. Значение по умолчанию - 2.
//
//	Задержка вычисляется по формуле:
//	  constant:   retry_delay
//	  linear:     retry_delay + retry_coeff * retryCount
//	  exponential: retry_delay + retry_coeff * retry_exp ^ retryCount
//
// где Count - номер текущей попытки (начиная с 0)
//
// Пример:
//
//	type MyEvent struct {
//	    addons.Retry
//	    UserID uint64
//	    // ...
//	}
//
//	tp.Register(q, tp.WithRetry(func (ctx context.Context, events []MyEvent) error {
//	    for _, event := range events {
//	        data, err := SelectUserByID(event.UserID)
//	        if err != nil {
//	            continue // пытаемся обработать событие неограниченное кол-во раз
//	                     // с постоянной задержкой defer_time.
//	                     // после выхода из обработчика для всех необработанных событий
//	                     // будет вызван Defer (одним запросом)
//	        }
//
//	        resp, err := SendToRemoteSystem(ctx, data)
//	        if err != nil {
//	            event.Retry() // пытаемся обработать указанное в конфиге кол-во раз
//	                          // задержка и её тип указаны в конфиге (retry_*)
//	        }
//
//	        // ...
//	        event.Done() // успешная обработка, после выхода из обработчика
//	                     // все обработанные события удаляются (одним запросом)
//	    }
//	}))
func WithRetry[EvType RetryableEvent, EvSlice ~[]EvType](handler Handler[EvType, EvSlice], opts ...RetryOption) Handler[EvType, EvSlice] {
	opt := &retryOpt{}

	for _, f := range opts {
		f(opt)
	}

	return func(ctx context.Context, events EvSlice) error {
		getLimiter := sync.OnceValues(func() (retryLimiter, error) {
			conf := events[0].Queue().Config()

			return newRetryLimiter(conf, events[0].QueueID(), "")
		})

		errs := []error{handler(ctx, events)}

		for _, event := range events {
			if !event.NeedRetry() {
				continue
			}

			// calling Done cancels the retry logic
			if event.IsDone() {
				continue
			}

			limiter, err := getLimiter()
			if err != nil {
				errs = append(errs, err)
				break // further processing is meaningless without a limiter
			}

			delay, ok := limiter.nextDelay(event.GetRetryCount())
			if !ok {
				if opt.onRetryExhausted != nil {
					opt.onRetryExhausted(event)
				}

				event.Done()
				incMetric(event, "tp_events_retry_exhausted")

				continue
			}

			event.IncRetryCount()
			incMetric(event, "tp_events_retried")

			if opt.onRetry != nil {
				opt.onRetry(event)
			}

			pubOpts := []queuedclient.PublishOption(nil)

			// don't apply calculated delay if Defer was called explicitly
			if !event.HasCustomDefer() {
				pubOpts = []queuedclient.PublishOption{queuedclient.PublishInitialDelay(delay)}
			}

			err = event.Queue().Replace(ctx, event, pubOpts...)
			if err != nil {
				errs = append(errs, err)
			}
		}

		return errors.Join(errs...)
	}
}

func incMetric(ev Event, name string) {
	if ms := ev.Queue().MetricsSet(); (ms != advmetricsset.Set{}) {
		ms.Name(name).Int("queue_id", int(ev.QueueID())).GetOrCreateCounter().Inc()
	}
}

type retryLimiter interface {
	nextDelay(uint32) (time.Duration, bool)
}

type constLimiter struct {
	limit uint32
	delay time.Duration
}

func (cl *constLimiter) nextDelay(count uint32) (time.Duration, bool) {
	if cl.limit <= count {
		return 0, false
	}

	return cl.delay, true
}

type linearLimiter struct {
	limit uint32
	delay time.Duration
	coeff time.Duration
}

func (ll *linearLimiter) nextDelay(count uint32) (time.Duration, bool) {
	if ll.limit <= count {
		return 0, false
	}

	return ll.delay + ll.coeff*time.Duration(count), true
}

type expLimiter struct {
	limit uint32
	delay time.Duration
	coeff time.Duration
	exp   float64
}

func (el *expLimiter) nextDelay(count uint32) (time.Duration, bool) {
	if el.limit <= count {
		return 0, false
	}

	return el.delay + el.coeff*time.Duration(math.Pow(el.exp, float64(count))), true
}

func newRetryLimiter(conf *queueconfig.Config, queueID uint16, retrySuffix string) (retryLimiter, error) {
	typ, err := conf.RetryType(queueID)
	if err != nil {
		return nil, err
	}

	limit, err := conf.RetryLimit(queueID, retrySuffix)
	if err != nil {
		return nil, err
	}

	delay, err := conf.RetryDelay(queueID, retrySuffix)
	if err != nil {
		return nil, err
	}

	if typ == queueconfig.RetryConstant {
		return &constLimiter{
			limit: limit,
			delay: delay,
		}, nil
	}

	coeff, err := conf.RetryCoeff(queueID, retrySuffix)
	if err != nil {
		return nil, err
	}

	if typ == queueconfig.RetryLinear {
		return &linearLimiter{
			limit: limit,
			delay: delay,
			coeff: coeff,
		}, nil
	}

	exp, err := conf.RetryExp(queueID, retrySuffix)
	if err != nil {
		return nil, err
	}

	return &expLimiter{
		limit: limit,
		delay: delay,
		coeff: coeff,
		exp:   exp,
	}, nil
}

// RetryableEvent2 - событие, встраивающее [addons.Retryable2].
// Поддерживает middleware [WithRetry2].
type RetryableEvent2 interface {
	Event
	IncRetry1Count()
	IncRetry2Count()
	GetRetry1Count() uint32
	GetRetry2Count() uint32
	Retry1()
	Retry2()
	NeedRetry1() bool
	NeedRetry2() bool
}

type Retry2Option func(*retry2Opt)

// OnRetry2 аналог OnRetry для WithRetry2
func OnRetry2[T RetryableEvent2](f func(T)) Retry2Option {
	return func(opt *retry2Opt) {
		opt.onRetry = func(ev RetryableEvent2) {
			f(ev.(T))
		}
	}
}

// OnRetry2Exhausted аналог OnRetryExhausted для WithRetry2.
// В обработчик так же передаётся имя счётчика — cnt
func OnRetry2Exhausted[T RetryableEvent2](f func(T, string)) Retry2Option {
	return func(opt *retry2Opt) {
		opt.onRetryExhausted = func(ev RetryableEvent2, cnt string) {
			f(ev.(T), cnt)
		}
	}
}

// Retry2CounterNames позволяет задать альтернативные имена для настроек счётчиков в онлайнконфе.
// По умолчанию, "1" и "2".
func Retry2CounterNames(retry1, retry2 string) Retry2Option {
	return func(opt *retry2Opt) {
		opt.retry1 = retry1
		opt.retry2 = retry2
	}
}

type retry2Opt struct {
	retry1           string
	retry2           string
	onRetry          func(RetryableEvent2)
	onRetryExhausted func(RetryableEvent2, string)
}

func WithRetry2[EvType RetryableEvent2, EvSlice ~[]EvType](handler Handler[EvType, EvSlice], opts ...Retry2Option) Handler[EvType, EvSlice] { //nolint:gocognit
	opt := retry2Opt{
		retry1: "1",
		retry2: "2",
	}

	for _, f := range opts {
		f(&opt)
	}

	return func(ctx context.Context, events EvSlice) error {
		getLimiter1 := sync.OnceValues(func() (retryLimiter, error) {
			conf := events[0].Queue().Config()
			return newRetryLimiter(conf, events[0].QueueID(), opt.retry1)
		})

		getLimiter2 := sync.OnceValues(func() (retryLimiter, error) {
			conf := events[0].Queue().Config()
			return newRetryLimiter(conf, events[0].QueueID(), opt.retry2)
		})

		errs := []error{handler(ctx, events)}

		for _, event := range events {
			if !event.NeedRetry1() && !event.NeedRetry2() {
				continue
			}

			// calling Done cancels the retry logic
			if event.IsDone() {
				continue
			}

			var delay time.Duration

			if event.NeedRetry1() {
				limiter1, err := getLimiter1()
				if err != nil {
					errs = append(errs, err)
					continue // the other limiter may be ok
				}

				delay1, ok := limiter1.nextDelay(event.GetRetry1Count())
				if !ok {
					if opt.onRetryExhausted != nil {
						opt.onRetryExhausted(event, opt.retry1)
					}

					event.Done()
					incMetric(event, "tp_events_retry_exhausted")

					continue
				}

				event.IncRetry1Count()
				incMetric(event, "tp_events_retried")

				delay = delay1
			}

			if event.NeedRetry2() {
				limiter2, err := getLimiter2()
				if err != nil {
					errs = append(errs, err)
					continue // the other limiter may be ok
				}

				delay2, ok := limiter2.nextDelay(event.GetRetry2Count())
				if !ok {
					if opt.onRetryExhausted != nil {
						opt.onRetryExhausted(event, opt.retry2)
					}

					event.Done()
					incMetric(event, "tp_events_retry_exhausted")

					continue
				}

				event.IncRetry2Count()
				incMetric(event, "tp_events_retried")

				if delay < delay2 {
					delay = delay2
				}
			}

			if opt.onRetry != nil {
				opt.onRetry(event)
			}

			pubOpts := []queuedclient.PublishOption(nil)

			// don't apply calculated delay if Defer was called explicitly
			if !event.HasCustomDefer() {
				pubOpts = []queuedclient.PublishOption{queuedclient.PublishInitialDelay(delay)}
			}

			err := event.Queue().Replace(ctx, event, pubOpts...)
			if err != nil {
				errs = append(errs, err)
			}
		}

		return errors.Join(errs...)
	}
}

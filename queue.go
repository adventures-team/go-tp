package tp

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	advmetricsset "github.com/my-mail-ru/go-adv-metrics/set"
	"github.com/my-mail-ru/go-iproto"

	"github.com/my-mail-ru/go-queuedproto"

	"gitlab.adv.team/go/tp.git/encodings"
	"gitlab.adv.team/go/tp.git/queueconfig"
	"gitlab.adv.team/go/tp.git/queuedclient"
	"gitlab.adv.team/go/tp.git/workerpool"
)

//go:generate go tool iprotogen -r -tests -tags integration

// MaxVersion - максимальная версия события (в верхних битах хранятся флаги)
const MaxVersion = queuedproto.VersionMask

type (
	// QueueIDType - тип id очереди. Используем type alias - просто для удобства восприятия, "настоящий" тип (с методами, и громоздким приведением типов) нам не нужен.
	QueueIDType = uint16

	// VersionType - id версии событий.
	VersionType = uint8 // из-за флагов в номере версии в перле, максимальный номер версии - 31 (queuedproto.FlagEnableAddons-1).

	// Queue - высокоуровневое API очередей.
	//
	// В комментариях описано поведение реализации, возвращаемой [NewQueue].
	Queue interface {
		// Client возвращает интерфейс низкоуровневого API очередей
		Client() QueuedClient

		// Config возвращает высокоуровневый конфиг очередей с возможностью определения значений
		// для каждой очереди, с возможностью задания глобальных и per-instance дефолтов.
		Config() *queueconfig.Config

		// NewPoller возвращает интерфейс для опроса событий из очереди.
		// Вынесен для возможности внешней реализации разных стратегий опроса очереди
		// (например, managed master).
		NewPoller(logger *zerolog.Logger) workerpool.Poller

		// IsRegistered проверяет, зарегистрирована ли указанная очередь вызовом Register.
		IsRegistered(queueID uint16) bool

		// Publish кодирует событие при помощи метода Encode, и отправляет его в очередь методом [queuedclient.Publish].
		// Клиент очередей должен быть проинициализирован.
		//
		// Возвращается ID события (сгенерённый методом BuildEventID(), либо возвращённый queued).
		//
		// Если у события есть метод IgnoreDuplicateItems, и он возвращает true, дубли при
		// совпадении ID события игнорируются (также подавляется ошибка в логе queued флагом queuedproto.FlagIgnoreWrongItem).
		Publish(ctx context.Context, event Event, opts ...queuedclient.PublishOption) (queuedproto.EventID, error)

		// Replace заменяет существующее событие в очереди. ID события должно быть обязательно задано.
		// Если время срабатывания не установлено через опции, оно не изменяется.
		Replace(ctx context.Context, event Event, opts ...queuedclient.PublishOption) error

		MetricsSet() advmetricsset.Set
		SetMetricsSet(advmetricsset.Set)
	}

	// QueuedClient - интерфейс низкоуровневого API очередей (клиента queued).
	// Реализован в [queuedclient.QueuedClient] (там же - более подробное описание методов).
	QueuedClient interface {
		// Publish отправляет событие в очередь.
		// Автоинкремент ID и способы задания задержки описаны в документации к пакету queuedclient.
		Publish(ctx context.Context, queueID uint16, eventID queuedproto.EventID, flags uint32, eventData []byte, opts ...queuedclient.PublishOption) (queuedproto.EventID, error)

		// Replace заменяет существующее событие в очереди.
		Replace(ctx context.Context, queueID uint16, eventID queuedproto.EventID, eventData []byte, opts ...queuedclient.PublishOption) (bool, error)

		// Defer устанавливает задержку обработки для указанных событий. Если задержка не указана (delay == 0), используется значение defer_time из конфига.
		Defer(ctx context.Context, queueID uint16, delay time.Duration, eventIDs []queuedproto.EventID) error

		// Delete удаляет указанные события параллельно изо всех необходимых шардов пачками по update_batch_size событий.
		Delete(ctx context.Context, queueID uint16, eventIDs []queuedproto.EventID) error

		// GetActive получает активные события из очереди queueID изо всех шардов одновременно.
		GetActive(ctx context.Context, queueID uint16, count int) (queuedproto.ItemList, error)

		// GetByIDs запрашивает события из очереди queueID с указанными ID из указанных шардов параллельно.
		GetByIDs(ctx context.Context, queueID uint16, eventIDs []queuedproto.EventID) (queuedproto.ItemList, error)

		// GetByIDsBroadcast запрашивает события из очереди queueID с указанными ID изо всех шардов одновременно.
		GetByIDsBroadcast(ctx context.Context, queueID uint16, eventIDs []uint64) (queuedproto.ItemList, error)

		// SetActivationTime устанавливает время активации для указанных событий.
		SetActivationTime(ctx context.Context, queueID uint16, atime time.Time, eventIDs []queuedproto.EventID) error

		// ShardNum возвращает количество сконфигурированных шардов queued.
		ShardNum() uint

		// MakeBroadcastRequest посылает запрос req параллельно во все шарды.
		MakeBroadcastRequest(ctx context.Context, req queuedproto.Request, resFunc queuedclient.ResultFunc) error

		// MakeMulticastRequest посылает запросы для указанных eventIDs в нужные шарды.
		MakeMulticastRequest(ctx context.Context, batchSize int, eventIDs []queuedproto.EventID, reqFunc queuedclient.RequestFunc, resFunc queuedclient.ResultFunc) error

		// MakeUnicastRequest посылает запрос req к указанному шарду.
		MakeUnicastRequest(ctx context.Context, shard uint, req queuedproto.Request) (usedShard uint, respBytes []byte, err error)

		MetricsSet() advmetricsset.Set
		SetMetricsSet(advmetricsset.Set)

		// IsOnline проверяет наличие соединения со всеми шардами.
		IsOnline() bool
	}

	// Builder - при поддержке этого интерфейса типом события, метод Build вызывается при добавлении события в очередь.
	// Можно использовать для автозаполнения часто используемых полей (время создания, etc).
	// Аналог в перле - builder/default (для отдельных полей)
	Builder interface {
		Build() error
	}

	// EventIDBuilder - события, поддерживающие этот интерфейс, могут генерить ID события сами (аналог в перле - generate_id).
	// По дефолту, ID событий генерит queued. Поля, автозаполненные при помощи Build(), можно использовать в методе BuildEventID()
	EventIDBuilder interface {
		BuildEventID() uint64
	}

	// Unpacker - если у события объявлен метод Unpack, он вызывается при поступлении события после его распаковки.
	// Можно использовать для извлечения полей, запакованных в EventID.
	Unpacker interface {
		Unpack() error
	}

	// IgnoreDuplicateItems - для событий, вернувших true, игнорируются дубли по ID сообщения (выставляется флаг FlagIgnoreWrongItem).
	// Следует использовать вместе с EventIDBuilder, если нужна идемпотентность ID.
	// Перловый аналог называется так же.
	IgnoreDuplicateItems interface {
		IgnoreDuplicateItems() bool
	}

	// DisableMarshaler - встраивание этого типа в структуру отключает сериализацию средствами iproto
	DisableMarshaler struct{}

	// MarshalerDisabled - для проверки, отключен ли у типа маршалер средствами DisableMarshaler
	MarshalerDisabled interface {
		MarshalerDisabled()
	}

	// Ломаем совместимость [BaseEvent] с маршалерами iproto и addons.
	// Достаточно объявить методы с несовместмой сигнатурой, тип так назван для лучшей читаемости.
	incompatible struct{}

	handlerMap map[QueueIDType]handlerVersionMap

	handlerVersionMap map[VersionType]handlerInfo // TODO map для 32 значений? переделать на массив (не слайс)?

	handlerInfo struct {
		newEvents func(int) any // возвращает слайс событий указанной ёмкости (длина 0)
		boundPtr  func(any) any // возвращает указатель на элемент, следующий после длины слайса. нельзя вызывать, если слайс наполнен целиком
		grow      func(any) any // увеличивает длину слайса на 1
		handler   func(context.Context, any) error
		format    encodings.Format
	}

	queueImpl struct {
		mu         sync.RWMutex
		handlerMap handlerMap
		client     QueuedClient
		metrics    advmetricsset.Set
		config     *queueconfig.Config
	}
)

// NewQueue создаёт объект для высокоуровневой работы с API очередей.
// Инициализирует клиент очередей ([queuedclient]) с указанным конфигом.
func NewQueue(ctx context.Context, conf *queueconfig.Config) (Queue, error) {
	client, err := queuedclient.Dial(ctx, conf)
	if err != nil {
		return nil, err
	}

	return &queueImpl{
		client: client,
		config: conf,
	}, nil
}

// Client возвращает клиент очередей.
//
// Это - API нижнего уровня, не стоит им пользоваться без необходимости.
//
// Для удаления событий из обработчиков стоит пользоваться методом BaseEvent.Done, для установки времени задержки - BaseEvent.Defer.
//
// Если будут нужны какие-то методы "среднего уровня", имеет смысл их добавить к типу Queue.
func (qi *queueImpl) Client() QueuedClient {
	return qi.client
}

// Config возвращает высокоуровневый конфиг очередей с возможностью определения значений для каждой очереди.
func (qi *queueImpl) Config() *queueconfig.Config {
	return qi.config
}

// Publish кодирует событие при помощи метода Encode, и отправляет его в очередь методом [queuedclient.Publish].
// Клиент очередей должен быть проинициализирован.
//
// Возвращается ID события (сгенерённый методом BuildEventID(), либо возвращённый queued).
//
// Если у события есть метод IgnoreDuplicateItems, и он возвращает true, дубли при
// совпадении ID события игнорируются (также подавляется ошибка в логе queued флагом queuedproto.FlagIgnoreWrongItem).
func (qi *queueImpl) Publish(ctx context.Context, event Event, opts ...queuedclient.PublishOption) (queuedproto.EventID, error) {
	eventData, err := EncodeEvent(event)
	if err != nil {
		return queuedproto.EventID{}, err
	}

	flags := uint32(0)

	if ign, ok := event.(IgnoreDuplicateItems); ok && ign.IgnoreDuplicateItems() {
		flags |= queuedproto.FlagIgnoreWrongItem
	}

	evID := event.GetEventID()
	origEvID := evID

	if getEvID, ok := event.(EventIDBuilder); ok {
		evID.ID = getEvID.BuildEventID()
	}

	if evID.ID == 0 {
		evID.Shard = queuedclient.BalanceShard
	} else {
		shardNum := qi.client.ShardNum()
		evID.Shard = CalcShard(evID.ID, shardNum)

		if evID.Shard >= shardNum {
			return queuedproto.EventID{}, fmt.Errorf("%T.Shard() returned %d, but it should be less than shard count %d", event, evID.Shard, shardNum)
		}
	}

	if evID != origEvID {
		if getBaseEv, ok := event.(baseEventGetter); ok {
			baseEvent := getBaseEv.getBaseEvent()
			baseEvent.EventID = evID // предпочтительный способ получения генерённого EventID - через возвращаемое значение. EventID в BaseEvent будет проставлен только если event передано по указателю, что не является обязательным.
		}
	}

	return qi.client.Publish(ctx, event.QueueID(), evID, flags, eventData, opts...)
}

// Replace заменяет существующее событие в очереди. ID события должно быть обязательно задано.
// Если время срабатывания не установлено через опции, оно не изменяется.
func (qi *queueImpl) Replace(ctx context.Context, event Event, opts ...queuedclient.PublishOption) error {
	eventData, err := EncodeEvent(event)
	if err != nil {
		return err
	}

	var (
		bev *BaseEvent
		qID QueueIDType
	)

	if bevGetter, ok := event.(baseEventGetter); ok {
		bev = bevGetter.getBaseEvent()

		if bev.list != nil { // XXX странный костыль, скорее всего, можно безболезненно выкинуть
			qID = bev.list.queueID
		}
	}

	if qID == 0 {
		qID = event.QueueID()
	}

	hasCustomDefer, err := qi.client.Replace(ctx, qID, event.GetEventID(), eventData, opts...)
	if err != nil {
		return err
	}

	if hasCustomDefer && bev != nil {
		bev.setHasCustomDefer()
	}

	return nil
}

// getActiveEvents получает активные события (наступило время срабатывания) в декодированном виде из очереди queueID. Если count не задано (==0), используется значение из конфига (fetch_batch_size).
//
// Вместе с ошибкой может вернуться и валидный (не-nil) результат, этот кейс надо учитывать. Это означает, что ошибку вернули только некоторые
// из шардов.
func (qi *queueImpl) getActiveEvents(ctx context.Context, queueID QueueIDType, count int) (*eventList, error) {
	return qi.eventListFromItems(ctx, queueID, func() (queuedproto.ItemList, error) {
		return qi.client.GetActive(ctx, queueID, count)
	})
}

/*
// getEventsByIDs получает события в декодированном виде с указанными ID из очереди queueID
//
// Вместе с ошибкой может вернуться и валидный (не-nil) результат, этот кейс надо учитывать. Это означает, что ошибку вернули только некоторые
// из шардов.
func (qi *queueImpl) getEventsByIDs(ctx context.Context, queueID QueueIDType, eventIDs []queuedproto.EventID) (*eventList, error) {
	return qi.eventListFromItems(ctx, queueID, func() (queuedproto.ItemList, error) {
		return qi.client.GetByIDs(ctx, queueID, eventIDs)
	})
}
*/

func (qi *queueImpl) eventListFromItems(ctx context.Context, queueID QueueIDType, get func() (queuedproto.ItemList, error)) (*eventList, error) {
	qi.mu.RLock()
	defer qi.mu.RUnlock()

	byVersion, ok := qi.handlerMap[queueID]
	if !ok {
		return nil, fmt.Errorf("%w %d", ErrUnknownQueue, queueID)
	}

	items, getErr := get()
	if getErr != nil && len(items.Items) == 0 {
		return nil, getErr
	}

	evl := &eventList{
		queueID: queueID,
		queue:   qi,
	}

	if len(items.Items) == 0 {
		return evl, nil
	}

	evl.baseEvents = make([]*BaseEvent, 0, len(items.Items))
	unsupported := make([]queuedproto.EventID, 0, len(items.Items))

	for _, item := range items.Items {
		if len(item.Data) == 0 {
			zerolog.Ctx(ctx).Error().Uint16("queue_id", queueID).Stringer("event_id", item.EventID).Msg("empty event received")
			unsupported = append(unsupported, item.EventID)

			continue
		}

		version := item.Data[0] & queuedproto.VersionMask
		hInfo, ok := byVersion[version]

		if !ok {
			zerolog.Ctx(ctx).Error().Uint16("queue_id", queueID).Stringer("event_id", item.EventID).Uint8("version", version).Msg("unsupported version")
			unsupported = append(unsupported, item.EventID)

			continue
		}

		sl := evl.eventsByVersion[version]
		if sl == nil {
			sl = hInfo.newEvents(len(items.Items))
			evl.eventsByVersion[version] = sl
		}

		evPtr := hInfo.boundPtr(sl)

		// [Register] ensures that the event (or a pointer to it) implements [iproto.Unmarshaler]
		decodeTo, ok := evPtr.(iproto.Unmarshaler)
		if !ok {
			zerolog.Ctx(ctx).Error().Msgf("type %T doesn't implement iproto.Unmarshaler (pointer receiver is required)", evPtr)

			unsupported = append(unsupported, item.EventID)

			continue
		}

		getBaseEv, ok := evPtr.(baseEventGetter)
		if !ok {
			zerolog.Ctx(ctx).Error().Stringer("event_id", item.EventID).Msgf("internal error: boundPtr returned non-pointer: %T", evPtr)
			unsupported = append(unsupported, item.EventID)

			continue
		}

		baseEvent := getBaseEv.getBaseEvent()

		if err := encodings.Decode(hInfo.format, item.Data, decodeTo); err != nil {
			zerolog.Ctx(ctx).Err(err).Uint16("queue_id", queueID).Stringer("event_id", item.EventID).Uint8("version", version).Send()
			unsupported = append(unsupported, item.EventID)

			continue
		}

		baseEvent.EventID = item.EventID
		baseEvent.list = evl
		baseEvent.flags = new(atomic.Uint32)

		if unpacker, ok := evPtr.(Unpacker); ok {
			if err := unpacker.Unpack(); err != nil {
				zerolog.Ctx(ctx).Err(err).Uint16("queue_id", queueID).Stringer("event_id", item.EventID).Uint8("version", version).Msg("Unpack() error")
				unsupported = append(unsupported, item.EventID)

				continue
			}
		}

		evl.baseEvents = append(evl.baseEvents, baseEvent)
		evl.eventsByVersion[version] = hInfo.grow(sl)
	}

	if len(unsupported) != 0 {
		if err := qi.client.Defer(ctx, queueID, 0, unsupported); err != nil {
			zerolog.Ctx(ctx).Err(err).Uint16("queue_id", queueID).Interface("event_ids", unsupported).Msg("failed to defer items")
		}
	}

	return evl, getErr
}

// register регистрирует обработчик для входщих событий из очереди queueID.
// Исключительно для внутреннего использования, юзеры должны региться функцией [Register].
// [Register] предоставляет функции-аргументы, работающие с конкретным юзерским типом.
// Рефлексия и unsafe не используются! Обёртка [Register] приводит всё к типам юзерских событий.
//
// hInfo.newEvents: конструктор нового слайса событий, аргумент - capacity, должен возвращать тип слайса юзерских событий.
// hInfo.boundPtr:   возвращает указатель на элемент с индексом длины слайса(!), т.е. "после последнего" (нарушения типобезопасности нет, поскольку элемент находится внутри capacity слайса).
// hInfo.grow:      увеличивает длину слайса на 1 элемент путём реслайсинга, данные не изменяются.
// hInfo.handler:   принимает слайс юзерских событий, assertion к нужному типу делается во враппере, подставляемом [Register].
//
// Повторная регистрация для той же очереди перезаписывает предыдущую, и не вызывает ошибку.
// Так сделано, чтобы не ломало регистрацию очередей из init() библиотек (что, тем не менее, делать не рекомендуется).
func (qi *queueImpl) register(queueID QueueIDType, hInfo handlerInfo, version uint8) error {
	if version > MaxVersion {
		return fmt.Errorf("%w: %d", ErrVersionTooBig, queueID)
	}

	qi.mu.Lock()
	defer qi.mu.Unlock()

	if qi.handlerMap == nil {
		qi.handlerMap = make(handlerMap)
	}

	byVersion, ok := qi.handlerMap[queueID]
	if !ok {
		byVersion = make(handlerVersionMap)
		qi.handlerMap[queueID] = byVersion
	}

	byVersion[version] = hInfo

	return nil
}

// Register регистрирует обработчик очереди для получения событий. Для отправки (Publish) регистрация не требуется.
// handler принимает слайс событий, поддерживающих интерфейс Event (конкретных типов - не слайс интерфейсов).
// ID очереди и версия события определяется методами QueueID и Version. Для одной очереди могут быть зарегистрированы
// обработчики событий нескольких версий.
func Register[EvType Event, EvSlice ~[]EvType](q Queue, handler func(context.Context, EvSlice) error) error {
	qi, ok := q.(*queueImpl)
	if !ok {
		return fmt.Errorf("tp.Register supports only the Queue implementation returned by tp.NewQueue, but %T was supplied", q)
	}

	var ev EvType // zero value of EvType

	if _, ok := any(&ev).(iproto.Unmarshaler); !ok {
		return fmt.Errorf("tp.Register: event type %T (or a pointer to it) must implement iproto.Unmarshaler to be registered", ev)
	}

	queueID := ev.QueueID()
	version := ev.Version()

	hInfo := handlerInfo{
		newEvents: func(capacity int) any {
			return make(EvSlice, 0, capacity)
		},
		boundPtr: func(slI any) any {
			sl := slI.(EvSlice)
			newEv := &sl[len(sl) : len(sl)+1][0]

			return newEv
		},
		grow: func(slI any) any {
			sl := slI.(EvSlice)
			return sl[0 : len(sl)+1]
		},
		handler: func(ctx context.Context, slI any) error {
			return handler(ctx, slI.(EvSlice))
		},
		format: encodings.Detect(ev),
	}

	return qi.register(queueID, hInfo, version)
}

// IsRegistered проверяет, зарегистрирован ли обработчик очереди.
func (qi *queueImpl) IsRegistered(queueID uint16) bool {
	qi.mu.RLock()
	defer qi.mu.RUnlock()

	_, ok := qi.handlerMap[queueID]

	return ok
}

func (qi *queueImpl) MetricsSet() advmetricsset.Set {
	return qi.client.MetricsSet()
}

func (qi *queueImpl) SetMetricsSet(ms advmetricsset.Set) {
	qi.client.SetMetricsSet(ms)
}

// EncodeEvent кодирует событие.
//
// Если нужно только отправлять события, регистрация не требуется (в отличие от предыдущих версий tp).
//
// При соответствии интерфейсам Builder и/или EventIDBuilder вызываются соответствующие методы.
//
// Кодировщику безразлично, передаётся указатель или значение, но для вызова методов по указанным интерфейсам
// нужно соблюдать правильный тип (тот, для которого объявлен метод - очевидно, для Builder нужен указатель).
//
// ID события, кроме интерфейса EventIDBuilder, можно также передать явно. Номер шарда будет вычислен из ID события,
// если оно задано одним из этих способов. Если EventID.ID == 0, шард будет выбран по round robin ([queuedclient.BalanceShard]).
func EncodeEvent(event Event) (eventData []byte, err error) {
	if event == nil {
		return nil, fmt.Errorf("%w nil event data", ErrUnknownType)
	}

	if builder, ok := event.(Builder); ok {
		if err = builder.Build(); err != nil {
			return nil, fmt.Errorf("%T: %w", event, err)
		}
	}

	return encodings.Encode(encodings.Detect(event), event.Version(), event)
}

var (
	_ iproto.MarshalerUnmarshaler = DisableMarshaler{}
	_ MarshalerDisabled           = DisableMarshaler{}
)

// MarshalIProto - не делает ничего
func (DisableMarshaler) MarshalIProto(buf []byte) ([]byte, error) {
	return buf, nil
}

// UnmarshalIProto - не делает ничего
func (DisableMarshaler) UnmarshalIProto(buf []byte) ([]byte, error) {
	return buf, nil
}

func (DisableMarshaler) MarshalerDisabled() {}

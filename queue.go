package tp

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"strconv"
	"sync/atomic"

	"github.com/rs/zerolog"

	"gitlab.corp.mail.ru/mygo/easygo.git/iproto"
	"gitlab.corp.mail.ru/mygo/queuedproto.git"
	"gitlab.corp.mail.ru/mygo/tp.git/encodings"
	"gitlab.corp.mail.ru/mygo/tp.git/queueconfig"
	"gitlab.corp.mail.ru/mygo/tp.git/queuedclient"
)

// MaxVersion - максимальная версия события (в верхних битах хранятся флаги)
const MaxVersion = queuedproto.VersionMask

type (
	// QueueIDType - тип id очереди. Используем type alias - просто для удобства восприятия, "настоящий" тип (с методами, и громоздким приведением типов) нам не нужен.
	QueueIDType = uint16

	// VersionType - id версии событий.
	VersionType = uint8 // из-за флагов в номере версии в перле, максимальный номер версии - 31 (queuedproto.FlagEnableAddons-1).

	// Queue - высокоуровневое API очередей. Содержит информацию об обрабатываемых очередях (см. Register)
	Queue struct {
		handlerMap handlerMap
		client     *queuedclient.QueuedClient
	}

	// Event - интерфейс обработчика очереди. Структура обработчика должна включать в себя тип BaseEvent
	Event interface {
		Version() VersionType // Возвращает версию события. Должен быть определён для типа события.
		QueueID() QueueIDType // Возвращает ID очереди. должен быть определён для типа события.

		getEventID() queuedproto.EventID
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
)

// Init создаёт объект для высокоуровневой работы с API очередей. Инициализирует клиент очередей (queuedclient) с указанным конфигом.
func Init(ctx context.Context, conf *queueconfig.Config) (*Queue, error) {
	client, err := queuedclient.Dial(ctx, conf)
	if err != nil {
		return nil, err
	}

	return &Queue{
		client: client,
	}, nil
}

// Queued возвращает клиент очередей.
//
// Это - API нижнего уровня, не стоит им пользоваться без необходимости.
//
// Для удаления событий из обработчиков стоит пользоваться методом BaseEvent.Done, для установки времени задержки - BaseEvent.Defer.
//
// Если будут нужны какие-то методы "среднего уровня", имеет смысл их добавить к типу Queue.
func (queue *Queue) Queued() *queuedclient.QueuedClient {
	return queue.client
}

// Publish кодирует событие при помощи метода Encode, и отправляет его в очередь методом queuedclient.Publish.
// Клиент очередей должен быть проинициализирован.
//
// Возвращается ID события (сгенерённый методом BuildEventID(), либо возвращённый queued).
//
// Если тип события поддерживает интерфейс IgnoreDuplicateItems, и метод IgnoreDuplicateItems возвращает true, дубли при
// совпадении ID события игнорируются (также подавляется ошибка в логе queued флагом queuedproto.FlagIgnoreWrongItem).
func (queue *Queue) Publish(ctx context.Context, event Event) (eventID queuedproto.EventID, err error) {
	eventData, err := queue.Encode(event)
	if err != nil {
		return queuedproto.EventID{}, err
	}

	flags := uint32(0)

	if ign, ok := event.(IgnoreDuplicateItems); ok && ign.IgnoreDuplicateItems() {
		flags |= queuedproto.FlagIgnoreWrongItem
	}

	evID := event.getEventID()
	origEvID := evID

	if getEvID, ok := event.(EventIDBuilder); ok {
		evID.ID = getEvID.BuildEventID()
	}

	if evID.ID == 0 {
		evID.Shard = queuedclient.BalanceShard
	} else {
		evID.Shard = CalcShard(evID.ID, queue.client.ShardNum())
	}

	if evID != origEvID {
		if getBaseEv, ok := event.(baseEventGetter); ok {
			baseEvent := getBaseEv.getBaseEvent()
			baseEvent.EventID = evID // предпочтительный способ получения генерённого EventID - через возвращаемое значение. EventID в BaseEvent будет проставлен только если event передано по указателю, что не является обязательным.
		}
	}

	return queue.client.Publish(ctx, event.QueueID(), event.getEventID(), flags, eventData)
}

// TODO тут нужны высокоуровневые методы Delete и Defer, чтобы не приходилось собирать вручную айдишник и лезть в низкоуровневый QueuedClient

// GetActiveEvents получает активные события (наступило время срабатывания) в декодированном виде из очереди queueID. Если count не задано (==0), используется значение из конфига (fetch_batch_size).
//
// Вместе с ошибкой может вернуться и валидный (не-nil) результат, этот кейс надо учитывать. Это означает, что ошибку вернули только некоторые
// из шардов.
func (queue *Queue) GetActiveEvents(ctx context.Context, queueID QueueIDType, count int) (*EventList, error) {
	return queue.eventListFromItems(ctx, queueID, func() (queuedproto.ItemList, error) {
		return queue.client.GetActive(ctx, queueID, count)
	})
}

// GetEventsByIDs получает события в декодированном виде с указанными ID из очереди queueID
//
// Вместе с ошибкой может вернуться и валидный (не-nil) результат, этот кейс надо учитывать. Это означает, что ошибку вернули только некоторые
// из шардов.
func (queue *Queue) GetEventsByIDs(ctx context.Context, queueID QueueIDType, eventIDs []queuedproto.EventID) (*EventList, error) {
	return queue.eventListFromItems(ctx, queueID, func() (queuedproto.ItemList, error) {
		return queue.client.GetByIDs(ctx, queueID, eventIDs)
	})
}

func (queue *Queue) eventListFromItems(ctx context.Context, queueID QueueIDType, get func() (queuedproto.ItemList, error)) (*EventList, error) {
	byVersion, ok := queue.handlerMap[queueID]
	if !ok {
		return nil, fmt.Errorf("%w %d", ErrUnknownQueue, queueID)
	}

	items, getErr := get()
	if getErr != nil && len(items.Items) == 0 {
		return nil, getErr
	}

	evl := &EventList{
		queueID: queueID,
		queue:   queue,
	}

	if len(items.Items) == 0 {
		return evl, nil
	}

	evl.baseEvents = make([]*BaseEvent, 0, len(items.Items))
	unsupported := make([]queuedproto.EventID, 0, len(items.Items))

	for _, item := range items.Items {
		if len(item.Data) == 0 {
			zerolog.Ctx(ctx).Error().Uint16("queue_id", queueID).Stringer("event_id", item).Msg("empty event received")
			unsupported = append(unsupported, item.EventID)

			continue
		}

		version := item.Data[0] & queuedproto.VersionMask
		hInfo, ok := byVersion[version]

		if !ok {
			zerolog.Ctx(ctx).Error().Uint16("queue_id", queueID).Stringer("event_id", item).Uint8("version", version).Msg("unsupported version")
			unsupported = append(unsupported, item.EventID)

			continue
		}

		sl := evl.eventsByVersion[version]
		if sl == nil {
			sl = hInfo.newEvents(len(items.Items))
			evl.eventsByVersion[version] = sl
		}

		event := hInfo.boundPtr(sl)

		getBaseEv, ok := event.(baseEventGetter)
		if !ok {
			zerolog.Ctx(ctx).Error().Stringer("event_id", item).Msg("internal error: boundPtr returned non-pointer")
			unsupported = append(unsupported, item.EventID)

			continue
		}

		baseEvent := getBaseEv.getBaseEvent()

		if err := encodings.Decode(hInfo.format, item.Data, event); err != nil {
			zerolog.Ctx(ctx).Err(err).Uint16("queue_id", queueID).Stringer("event_id", item).Uint8("version", version).Send()
			unsupported = append(unsupported, item.EventID)

			continue
		}

		baseEvent.EventID = item.EventID
		baseEvent.list = evl
		baseEvent.flags = new(atomic.Uint32)

		if unpacker, ok := event.(Unpacker); ok {
			if err := unpacker.Unpack(); err != nil {
				zerolog.Ctx(ctx).Err(err).Uint16("queue_id", queueID).Stringer("event_id", item).Uint8("version", version).Msg("Unpack() error")
				unsupported = append(unsupported, item.EventID)

				continue
			}
		}

		evl.baseEvents = append(evl.baseEvents, baseEvent)
		evl.eventsByVersion[version] = hInfo.grow(sl)
	}

	if len(unsupported) != 0 {
		if err := queue.client.Defer(ctx, queueID, 0, unsupported); err != nil {
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
func (q *Queue) register(queueID QueueIDType, hInfo handlerInfo, version uint8) error {
	if version > MaxVersion {
		return fmt.Errorf("%w: %d", ErrVersionTooBig, queueID)
	}

	if q.handlerMap == nil {
		q.handlerMap = make(handlerMap)
	}

	byVersion, ok := q.handlerMap[queueID]
	if !ok {
		byVersion = make(handlerVersionMap)
		q.handlerMap[queueID] = byVersion
	}

	byVersion[version] = hInfo

	return nil
}

// Register регистрирует обработчик очереди для получения событий. Для отправки регистрация не требуется.
// handler принимает слайс событий, поддерживающих интерфейс Event (конкретных типов - не слайс интерфейсов).
// ID очереди и версия события определяется методами QueueID и Version. Для одной очереди могут быть зарегистрированы
// обработчики событий нескольких версий.
func Register[EvType Event](q *Queue, handler func(context.Context, []EvType) error) error {
	var ev EvType // zero value of EvType

	queueID := ev.QueueID()
	version := ev.Version()

	hInfo := handlerInfo{
		newEvents: func(cap int) any {
			return make([]EvType, 0, cap)
		},
		boundPtr: func(slI any) any {
			sl := slI.([]EvType)
			newEv := &sl[len(sl) : len(sl)+1][0]

			return newEv
		},
		grow: func(slI any) any {
			sl := slI.([]EvType)
			return sl[0 : len(sl)+1]
		},
		handler: func(ctx context.Context, slI any) error {
			return handler(ctx, slI.([]EvType))
		},
		format: encodings.Detect(ev),
	}

	return q.register(queueID, hInfo, version)
}

// Encode кодирует событие.
//
// Если нужно только отправлять события, регистрация не требуется (в отличие от предыдущих версий tp).
//
// При соответствии интерфейсам Builder и/или EventIDBuilder вызываются соответствующие методы.
//
// Кодировщику безразлично, передаётся указатель или значение, но для вызова методов по указанным интерфейсам
// нужно соблюдать правильный тип (тот, для которого объявлен метод - очевидно, для Builder нужен указатель).
//
// ID события, кроме интерфейса EventIDBuilder, можно также передать явно. Номер шарда будет вычислен из ID события,
// если оно задано одним из этих способов. Если EventID.ID == 0, шард будет выбран по round robin (queuedclient.BalanceShard).
func (q *Queue) Encode(event Event) (eventData []byte, err error) {
	if event == nil {
		return nil, fmt.Errorf("%w nil event data", ErrUnknownType)
	}

	if builder, ok := event.(Builder); ok {
		if err = builder.Build(); err != nil {
			return nil, fmt.Errorf("%T: %w", event, err)
		}
	}

	eventData, err = encodings.Encode(encodings.Detect(event), event.Version(), event)

	return
}

// IsRegistered проверяет, зарегистрирован ли обработчик очереди.
func (q *Queue) IsRegistered(queueID uint16) bool {
	_, ok := q.handlerMap[queueID]
	return ok
}

// CalcShard распределяет события по шардам в зависимости от хеша от ID события. Совместимо с перлом (алгоритм хеширования - crc от строкового десятичного представления eventID, распределение по шардам - остаток от деления).
func CalcShard(eventID uint64, shardNum uint) uint {
	hash := uint64(crc32.ChecksumIEEE([]byte(strconv.FormatUint(eventID, 10))))
	return uint(hash % uint64(shardNum))
}

var (
	_ iproto.IprotoPacker   = DisableMarshaler{}
	_ iproto.IprotoUnpacker = DisableMarshaler{}
	_ MarshalerDisabled     = DisableMarshaler{}
)

// IprotoPack - не делает ничего
func (DisableMarshaler) IprotoPack(w []byte, _ iproto.PackMode) ([]byte, error) {
	return w, nil
}

// IprotoUnpack - не делает ничего
func (DisableMarshaler) IprotoUnpack(*bytes.Reader, iproto.PackMode) error {
	return nil
}

func (DisableMarshaler) MarshalerDisabled() {}

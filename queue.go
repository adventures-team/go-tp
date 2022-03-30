package tp

import (
	"context"
	"fmt"
	"reflect"

	"github.com/funny-falcon/go-iproto/marshal"
	"github.com/rs/zerolog"

	"gitlab.corp.mail.ru/mygo/queuedproto.git"
	"gitlab.corp.mail.ru/mygo/tp.git/encodings"
	"gitlab.corp.mail.ru/mygo/tp.git/queueconfig"
	"gitlab.corp.mail.ru/mygo/tp.git/queuedclient"
)

type (
	// Queue - высокоуровневое API очередей. Содержит информацию об обрабатываемых очередях (см. Register)
	Queue struct {
		QueueGroup
		logger *zerolog.Logger
		client *queuedclient.QueuedClient
	}

	// Handler - интерфейс обработчика очереди. Структура обработчика должна включать в себя тип Event
	Handler interface {
		GetEvent() *Event             // Реализуется типом Event
		Version() VersionType         // Возвращает версию события. Вызывается только при регистрации
		Handle(context.Context) error // Реализуется юзером
		enforceDictatorship()         // Заставляем разработчиков юзать определённые тут типы Event и NoHandler, чтобы не намудрили. насилие во благо
	}

	// NoHandler - если для очереди (пока) не предусмотрена обработка (например, обработчик реализован на перле),
	// надо встроить в структуру события этот тип
	NoHandler struct{}

	noHandler interface {
		noHandler()
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

	// IgnoreDuplicateItems - для событий, вернувших true, игнорируются дубли по ID сообщения (выставляется флаг FlagIgnoreWrongItem).
	// Следует использовать вместе с EventIDBuilder, если нужна идемпотентность ID.
	// Перловый аналог называется так же.
	IgnoreDuplicateItems interface {
		IgnoreDuplicateItems() bool
	}

	// DisableMarshaler - встраивание этого типа в структуру отключает сериализацию средствами go-iproto
	DisableMarshaler struct{}

	// marshalerDisabled - для проверки, отключен ли у типа маршалер средствами DisableMarshaler
	marshalerDisabled interface {
		marshalerDisabled()
	}

	// Ломаем совместимость NoHandler и Event с маршалерами iproto и addons.
	// В принципе, достаточно объявить методы с несовместмой сигнатурой, поэтому тип (приватный,
	// да ещё и с таким названием) нужен лишь для устрашения разработчика.
	incompatible struct{}
)

// New создаёт объект для высокоуровневой работы с API очередей. Инициализирует клиент очередей (queuedclient) с указанным конфигом.
func New(conf *queueconfig.Config, logger *zerolog.Logger) (*Queue, error) {
	client, err := queuedclient.New(conf, logger)
	if err != nil {
		return nil, err
	}

	return &Queue{
		client: client,
		logger: logger,
	}, nil
}

// Queued возвращает клиент очередей.
//
// Это - API нижнего уровня, не стоит им пользоваться без необходимости.
//
// Для удаления событий из обработчиков стоит пользоваться методом Event.Done, для установки времени задержки - Event.Defer.
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
func (queue *Queue) Publish(data Handler) (eventID uint64, err error) {
	queueID, eventID, eventData, err := queue.Encode(data)
	if err != nil {
		return 0, err
	}

	flags := uint32(0)

	if ign, ok := data.(IgnoreDuplicateItems); ok && ign.IgnoreDuplicateItems() {
		flags |= queuedproto.FlagIgnoreWrongItem
	}

	return queue.client.Publish(queueID, eventID, flags, eventData)
}

// TODO тут нужны высокоуровневые методы Delete и Defer, чтобы не приходилось собирать вручную айдишник и лезть в низкоуровневый QueuedClient

// GetActiveEvents получает активные события (наступило время срабатывания) в декодированном виде из очереди queueID. Если count не задано (==0), используется значение из конфига (fetch_batch_size)
func (queue *Queue) GetActiveEvents(queueID QueueIDType, count int) (*eventList, error) {
	return queue.eventListFromItems(queueID, func() (queuedproto.ItemList, error) {
		return queue.client.GetActive(queueID, count)
	})
}

// GetEventsByIDs получает события в декодированном виде с указанными ID из очереди queueID
func (queue *Queue) GetEventsByIDs(queueID QueueIDType, eventIDs []uint64) (*eventList, error) {
	return queue.eventListFromItems(queueID, func() (queuedproto.ItemList, error) {
		return queue.client.GetByIDs(queueID, eventIDs)
	})
}

func (queue *Queue) eventListFromItems(queueID QueueIDType, get func() (queuedproto.ItemList, error)) (*eventList, error) {
	byVersion, ok := queue.incoming[queueID]
	if !ok {
		return nil, fmt.Errorf("%w %d", ErrUnknownQueue, queueID)
	}

	items, err := get()
	if err != nil {
		return nil, err
	}

	evl := &eventList{
		queueID: queueID,
		queue:   queue,
	}

	if len(items.Items) == 0 {
		return evl, nil
	}

	evl.events = make([]Handler, 0, len(items.Items))
	unsupported := make([]uint64, 0, len(items.Items))
	prevHandler := make(map[VersionType]*Handler, MaxVersion)

	for _, item := range items.Items {
		if len(item.Data) == 0 {
			// TODO пофиксить логи - протащить контекст всюду, или куда надо, решить, как будет не громоздко, и при этом без лишнего разнообразия, вызывающего путаницу
			queue.logger.Error().Uint16("queue_id", queueID).Uint64("event_id", item.ID).Msg("empty event received")
			unsupported = append(unsupported, item.ID)

			continue
		}

		version := item.Data[0] & queuedproto.VersionMask
		hinfo, ok := byVersion[version]

		if !ok {
			queue.logger.Error().Uint16("queue_id", queueID).Uint64("event_id", item.ID).Uint8("version", version).Msg("unsupported version")
			unsupported = append(unsupported, item.ID)

			continue
		}

		valPtr := reflect.New(hinfo.dataType)
		handler, ok := valPtr.Interface().(Handler)

		if !ok {
			return nil, ErrInternalHandler
		}

		if hinfo.autofillIdx != -1 {
			valPtr.Elem().Field(hinfo.autofillIdx).Set(hinfo.autofillVal)
		}

		if err := encodings.Decode(hinfo.format, item.Data, handler); err != nil {
			queue.logger.Err(err).Uint16("queue_id", queueID).Uint64("event_id", item.ID).Uint8("version", version).Send()
			unsupported = append(unsupported, item.ID)

			continue
		}

		ev := handler.GetEvent()
		ev.id = item.ID
		ev.list = evl

		if prevPtr, ok := prevHandler[version]; ok {
			*prevPtr = handler
		}

		prevHandler[version] = &ev.next

		evl.events = append(evl.events, handler)
	}

	if len(unsupported) != 0 {
		if err := queue.client.Defer(queueID, 0, unsupported); err != nil {
			queue.logger.Err(err).Uint16("queue_id", queueID).Uints64("event_ids", unsupported).Msg("failed to defer items")
		}
	}

	return evl, nil
}

var (
	_ marshal.IShortReader = DisableMarshaler{}
	_ marshal.IShortWriter = DisableMarshaler{}
	_ marshalerDisabled    = DisableMarshaler{}
)

// IRead - не делает ничего
func (DisableMarshaler) IRead(*marshal.Reader) {}

// IWrite - не делает ничего
func (DisableMarshaler) IWrite(*marshal.Writer) {}

func (DisableMarshaler) marshalerDisabled() {}

// Handle - не делает ничего
func (NoHandler) Handle(context.Context) error {
	return nil
}

// GetEvent возвращает пустое событие
func (NoHandler) GetEvent() *Event {
	return &Event{}
}

func (NoHandler) noHandler() {}

// IWrite - ломаем совместимость NoHandler с маршалерами, чтобы маршалер из вложенной структуры (например,
// DisableMarshaler из структуры объектов API, вложенный в событие), не повлиял на маршалинг события.
//
// Почему бы не объявить приватный тип с запрещёнными маршалерами, и не встроить его в типы NoHandler и Event?
// Зачем мозолить глаза разработчику, читающему доку?
//
// По правилам проброса встроенных полей в го: x.f denotes the field or method at the shallowest depth
// in T where there is such an f. If there is not exactly one f with shallowest depth, the selector
// expression is illegal.
//
// Т.е. если мы объявим "запрещающие" методы глубже (структура, вложенная в структуру Event/NoHandler) не перекроют
// методы, объявленные непосредственно у вложенной структуры 1го уровня.
//
// Если же всё-таки зачем-то потребуется объявить маршалеры для типа события - это можно сделать, т.к. они перекроют
// "наследуемые" методы (и с правильной, и с неправильной сигнатурой).
func (NoHandler) IWrite(incompatible) {}

// IRead - см. описание NoHandler.IWrite
func (NoHandler) IRead(incompatible) {}

// MarshalAddon - см. описание NoHandler.IWrite
func (NoHandler) MarshalAddon(incompatible) {}

// UnmarshalAddon - см. описание NoHandler.IWrite
func (NoHandler) UnmarshalAddon(incompatible) {}

// AddonID - см. описание NoHandler.IWrite
func (NoHandler) AddonID(incompatible) {}

func (NoHandler) marshalerDisabled(incompatible) {} //nolint:unused
func (NoHandler) enforceDictatorship()           {}

package tp

import (
	"fmt"
	"reflect"

	"gitlab.corp.mail.ru/mygo/queuedproto.git"
	"gitlab.corp.mail.ru/mygo/tp.git/encodings"
	"gitlab.corp.mail.ru/mygo/tp.git/queuedclient"
)

// MaxVersion - максимальная версия события (в верхних битах хранятся флаги)
const MaxVersion = queuedproto.VersionMask

type (
	// queueGroup - информация о всех зарегистрированных очередях. Мы сможем обрабатывать и посылать события для очередей, определённых тут.
	queueGroup struct {
		incoming incomingMap // мэп типов для входящих событий по id очереди и номеру версии
		outgoing outgoingMap // мэп id очереди и номеров версий по типу события
	}

	// QueueIDType - тип id очереди. Используем type alias - просто для удобства восприятия, "настоящий" тип (с методами, и громоздким приведением типов) нам не нужен.
	QueueIDType = uint16

	// VersionType - id версии событий. Тоже алиас.
	VersionType = uint8 // из-за флагов в номере версии в перле, максимальный номер версии - 31 (queuedproto.FlagEnableAddons-1).

	incomingMap map[QueueIDType]versionsMap

	versionsMap map[VersionType]handlerInfo // TODO map для 32 значений? переделать на массив (не слайс)?

	handlerInfo struct {
		dataType    reflect.Type  // Тип самой структуры, а не указателя на неё. Поддерживает интерфейс Handler
		autofillIdx int           // -1 - автозаполнение отключено
		autofillVal reflect.Value // поддерживает MarshalerDisabled
		format      encodings.Format
	}

	// outgoingMap - мэппинг типов объектов события в id очереди/версию события. Тип - самой структуры, а не указателя на неё.
	// создаём сразу два ключа - для типа (структуры), и для указателя на него, для ускорения кодировщика.
	outgoingMap map[reflect.Type]outgoingInfo // мэп по интерфейсу - не всегда хорошо (т.к. сравнивается указатель, а не значение), но в случае с reflect.Type такое допустимо (см. доку по нему)

	// outgoingInfo - id очереди и номер версии, используются для отправки событий
	outgoingInfo struct {
		queueID     QueueIDType
		format      encodings.Format
		version     VersionType
		autofillIdx int // -1 - автозаполнение отключено
		autofillVal reflect.Value
	}
)

// Register регистрирует тип valueExample (используется только тип, значение игнорируется) для версии version и очереди queueID.
// valueExample может быть как указателем, так и значением.
// Предполагается, что метод вызывается при инициализации, поэтому не поддерживает работу из параллельных горутин.
// Перед регистрацией типа события происходит попытка его закодировать, для обнаружения ошибок в определении структуры на раннем этапе (при запуске сервиса).
//
// Если в структуру valueExample встроен тип NoHandler, обработчики не регистрируются, и очередь можно использовать
// только для публикации новых событий (например, если обработка осуществляется в перле). Таким образом можно
// избежать случайного запуска обработки необрабатываемых событий при ошибке в конфиге.
//
// При обработки событий, параметр autofill будет записан в поле структуры события с таким же типом. Если полей
// с таким типом несколько, будет заполнено первое из них. В тип нужно встроить DisableMarshaler для избежания
// утечки содержимого. Use case - проброс объектов "уровня приложения" (доступ к базам, и т.д.) обработчикам.
// Поле должно быть экспортируемым (публичным).
//
// Если такая фича не требуется (таких объектов нет, либо используются глобальные переменные, либо очередь
// без обработчиков), следует передавать autofill = nil.
//
// Повторная регистрация для той же очереди перезаписывает предыдущую, и не вызывает ошибку.
// Так сделано, чтобы не ломало регистрацию очередей из init() библиотек (что, тем не менее, делать не рекомендуется).
func (q *Queue) Register(queueID QueueIDType, valueExample Handler, autofill MarshalerDisabled) error {
	version := valueExample.Version()

	if version > MaxVersion {
		return fmt.Errorf("%w: %d", ErrVersionTooBig, queueID)
	}

	evType := reflect.Indirect(reflect.ValueOf(valueExample)).Type()
	autofillIdx, autofillVal, err := setAutofill(autofill, evType)

	if err != nil {
		return err
	}

	format := encodings.Detect(valueExample)

	oi := outgoingInfo{
		queueID:     queueID,
		format:      format,
		version:     version,
		autofillIdx: autofillIdx,
		autofillVal: autofillVal,
	}

	// проверяем, что всё кодируется правильно, + наполняем кеш перл.аддонов
	if _, err := oi.encode(q, valueExample); err != nil {
		return err
	}

	if q.outgoing == nil {
		q.outgoing = make(outgoingMap)
	}

	q.outgoing[evType] = oi
	q.outgoing[reflect.PtrTo(evType)] = oi

	if _, ok := valueExample.(noHandler); ok {
		return nil
	}

	if q.incoming == nil {
		q.incoming = make(incomingMap)
	}

	byVersion, ok := q.incoming[queueID]
	if !ok {
		byVersion = make(versionsMap)
		q.incoming[queueID] = byVersion
	}

	hinfo := handlerInfo{
		dataType:    evType,
		autofillIdx: autofillIdx,
		autofillVal: autofillVal,
		format:      format,
	}

	byVersion[version] = hinfo

	return nil
}

// Encode кодирует событие. Тип события должен быть зарегистрирован при помощи метода Register.
//
// При соответствии интерфейсам Builder и/или EventIDBuilder вызываются соответствующие методы.
//
// Кодировщику безразлично, передаётся указатель или значение, но для вызова методов по указанным интерфейсам
// нужно соблюдать правильный тип (тот, для которого объявлен метод - очевидно, для Builder нужен указатель).
//
// ID события, кроме интерфейса EventIDBuilder, можно также передать явно. Номер шарда будет вычислен из ID события,
// если оно задано одним из этих способов. Если EventID.ID == 0, шард будет выбран по round robin (queuedclient.BalanceShard).
func (q *Queue) Encode(data Handler) (eventData []byte, err error) {
	if data == nil {
		return nil, fmt.Errorf("%w nil data", ErrUnknownType)
	}

	typ := reflect.TypeOf(data)
	oi, ok := q.outgoing[typ]

	if !ok {
		return nil, fmt.Errorf("%w %v", ErrUnknownType, typ)
	}

	return oi.encode(q, data)
}

func (q *Queue) IsRegistered(queueID uint16) bool {
	_, ok := q.incoming[queueID]
	return ok
}

func (oi *outgoingInfo) encode(queue *Queue, data Handler) (eventData []byte, err error) {
	if oi.autofillIdx != -1 {
		reflect.Indirect(reflect.ValueOf(data)).Field(oi.autofillIdx).Set(oi.autofillVal)
	}

	ev := data.GetEvent()

	if ev.list == nil {
		ev.list = &EventList{
			queueID: oi.queueID,
			queue:   queue,
		}
	}

	if builder, ok := data.(Builder); ok {
		if err = builder.Build(); err != nil {
			return nil, fmt.Errorf("%T: %w", data, err)
		}
	}

	if getEvID, ok := data.(EventIDBuilder); ok {
		ev.EventID.ID = getEvID.BuildEventID()
	}

	if ev.EventID.ID == 0 {
		ev.EventID.Shard = queuedclient.BalanceShard
	} else {
		ev.EventID.Shard = ev.Shard(ev.Hash())
	}

	eventData, err = encodings.Encode(oi.format, oi.version, data)

	ev.EventID.ID = 0

	return
}

func setAutofill(autofill MarshalerDisabled, evType reflect.Type) (autofillIdx int, autofillVal reflect.Value, err error) {
	if autofill == nil {
		return -1, reflect.Value{}, nil
	}

	if evType.Kind() != reflect.Struct {
		return -1, reflect.Value{}, fmt.Errorf("%v: %w", evType, ErrStructRequired)
	}

	autofillVal = reflect.ValueOf(autofill)
	autofillType := autofillVal.Type()

	fieldNum := evType.NumField()
	for i := 0; i < fieldNum; i++ {
		field := evType.Field(i)

		if field.Type == autofillType {
			if !field.IsExported() {
				return -1, reflect.Value{}, fmt.Errorf("%v.%s: %w", evType, field.Name, ErrExportedRequired)
			}

			return i, autofillVal, nil
		}
	}

	return -1, reflect.Value{}, fmt.Errorf("%v: %w with type %v", evType, ErrFieldNotFound, autofillType)
}

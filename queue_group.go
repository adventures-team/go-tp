package tp

import (
	"fmt"
	"reflect"

	"gitlab.corp.mail.ru/mygo/queuedproto.git"
	"gitlab.corp.mail.ru/mygo/tp.git/encodings"
)

// MaxVersion - максимальная версия события (в верхних битах хранятся флаги)
const MaxVersion = queuedproto.VersionMask

type (
	// QueueGroup - информация о всех зарегистрированных очередях. Мы сможем обрабатывать и посылать события для очередей, определённых тут.
	QueueGroup struct {
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
		autofillVal reflect.Value // поддерживает marshalerDisabled
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
// TODO сделать ошибку про повторную регистрацию, либо написать в доке, что повторная регистрация отменяет предыдущую.
func (qg *QueueGroup) Register(queueID QueueIDType, valueExample Handler, autofill marshalerDisabled) error {
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
	if _, _, _, err := oi.encode(valueExample); err != nil {
		return err
	}

	if qg.outgoing == nil {
		qg.outgoing = make(outgoingMap)
	}

	qg.outgoing[evType] = oi
	qg.outgoing[reflect.PtrTo(evType)] = oi

	if _, ok := valueExample.(noHandler); ok {
		return nil
	}

	if qg.incoming == nil {
		qg.incoming = make(incomingMap)
	}

	byVersion, ok := qg.incoming[queueID]
	if !ok {
		byVersion = make(versionsMap)
		qg.incoming[queueID] = byVersion
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
func (qg *QueueGroup) Encode(data Handler) (queueID QueueIDType, eventID uint64, eventData []byte, err error) {
	if data == nil {
		return 0, 0, nil, fmt.Errorf("%w nil data", ErrUnknownType)
	}

	typ := reflect.TypeOf(data)
	oi, ok := qg.outgoing[typ]

	if !ok {
		return 0, 0, nil, fmt.Errorf("%w %v", ErrUnknownType, typ)
	}

	return oi.encode(data)
}

func (oi *outgoingInfo) encode(data Handler) (queueID QueueIDType, eventID uint64, eventData []byte, err error) {
	if oi.autofillIdx != -1 {
		reflect.Indirect(reflect.ValueOf(data)).Field(oi.autofillIdx).Set(oi.autofillVal)
	}

	if builder, ok := data.(Builder); ok {
		if err = builder.Build(); err != nil {
			return 0, 0, nil, fmt.Errorf("%T: %w", data, err)
		}
	}

	if getEvID, ok := data.(EventIDBuilder); ok {
		eventID = getEvID.BuildEventID()
	}

	eventData, err = encodings.Encode(oi.format, oi.version, data)
	if err != nil {
		return 0, 0, nil, err
	}

	return oi.queueID, eventID, eventData, nil
}

func setAutofill(autofill marshalerDisabled, evType reflect.Type) (autofillIdx int, autofillVal reflect.Value, err error) {
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
			//if !field.IsExported() { // TODO go1.17+ - после апгрейда версии заменить на более понятный вызов
			if field.PkgPath != "" {
				return -1, reflect.Value{}, fmt.Errorf("%v.%s: %w", evType, field.Name, ErrExportedRequired)
			}

			return i, autofillVal, nil
		}
	}

	return -1, reflect.Value{}, fmt.Errorf("%v: %w with type %v", evType, ErrFieldNotFound, autofillType)
}

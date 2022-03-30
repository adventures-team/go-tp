package encodings

import (
	"fmt"
	"reflect"
	"sync"

	"gitlab.corp.mail.ru/mygo/queuedproto.git"
	"gitlab.corp.mail.ru/mygo/queuedproto.git/addons"
)

// В рантайме используется только быстрая рефлексия (reflect.Value.Field), при первом обращении к типу данные о номерах полей кешируются

// переделал слайс на мэп, чтобы юзер мог добавлять свои аддоны, не вторгаясь в пакет queuedproto/addons.
// отдельный слайс, упорядоченный по полям, нужен для воспроизводимого кодирования.
//type eventTypeAddons []int
type (
	addonMap map[uint16]int // addonID -> userEventFieldIdx

	addonField struct {
		addonID  uint16
		fieldIdx int
	}

	eventTypeAddons struct {
		addonMap    addonMap
		addonFields []addonField
	}
)

// для каждого типа тут хранится две записи - для типа значения, и указателя, чтобы не городить ифы.
// значение nil - аддонов у типа нет.
var eventTypeAddonCache = &sync.Map{} // reflect.TypeOf(UserEventStruct) -> eventTypeAddons

/*
Этот тип отсутствуют в пакете addons умышленно, чтобы не вводить в заблуждение юзеров!
Которые могут решить, что у MarshalAddon обязательно должен быть value receiver.
Пусть делают, как удобно/быстро (для толстых структур лучше все методы вызывать по указателю).

Здесь же этот интерфейс нужен вот почему - в структуру аддоны вложены по значению, и из-за
метода UnmarshalAddon, которому требуется pointer receiver, они не поддерживают интерфейс
addons.PerlAddon. Для декодера всё просто - структура и так приходит по указателю, поэтому
можно его получить методом reflect.Value.Addr. Но для энкодера такое закончится паникой,
т.к. структура туда может передаваться в т.ч. по значению.

Поэтому в энкодере мы требуем соответствие только этому интерфейсу (т.к. другие методы нам не нужны).

С getEventTypeAddons же всё сильно хитрее. Можно, конечно, попросить соответствие интерфейсу, который содержит только
методы AddonID и MarshalAddon, но тогда возможна проблема при стечении следующих обстоятельств:
- Юзер не знает про идиому с валидацией соответствия типа интерфейсу: var _ addons.PerlAddon = &MyAddon{}
- Метод UnmarshalAddon он не реализовал, или реализовал с неправильной сигнатурой
- Первым делом, вызвал энкодер, и инфа об этом недоделанном типе закешировалась
- Далее, вызвал энкодер. И словил панику на Addr().

Я, конечно, могу впилить проверку на CanAdd(), но это будет рантайм-проверка, хотелось же, чтобы их не было.
Поэтому в getEventTypeAddons реализован такой хак - если передано значение, а не указатель - я копирую структуру в новое
значение, и проверяю интерфейс у указателя на него. Медленно - но это редко выполнимое кешируемое действие.

А в энкодере/декодере всё уже работает быстро, с полной уверенностью соответствию интерфейсам.

P.S. Ведь можно в любом случае получить указатель на поле структуры?! Почему в reflect это не предусмотрели?
*/
type addonMarshaler interface {
	MarshalAddon() ([]byte, error)
}

var _ addonMarshaler = addons.PerlAddon(nil)

// decodeAddonsData декодирует данные аддонов в юзерскую структуру события eventPtr
func decodeAddonsData(eventPtr interface{}, addonsFromEvent []queuedproto.PerlAddonData) error {
	supportedAddons := getEventTypeAddons(eventPtr)
	if supportedAddons == nil {
		return nil
	}

	eventVal := reflect.ValueOf(eventPtr).Elem() // TODO грохнется, если передать не указатель. см. TODO в README.md, после реализации - убрать этот коммент

	for _, evAddon := range addonsFromEvent {
		fieldIdx, ok := supportedAddons.addonMap[evAddon.ID]
		if !ok {
			continue // неизвестные аддоны из полученного события просто игнорируем
		}

		// см. коммент перед type addonMarshaler.
		// здесь должен быть только указатель, поэтому Addr() не упадёт
		if err := eventVal.Field(fieldIdx).Addr().Interface().(addons.PerlAddon).UnmarshalAddon(evAddon.Data); err != nil {
			return fmt.Errorf("%T: %w: AddonID=%d", eventPtr, err, evAddon.ID)
		}
	}

	return nil
}

func encodeAddonsData(event interface{}) ([]queuedproto.PerlAddonData, error) {
	supportedAddons := getEventTypeAddons(event)
	if supportedAddons == nil {
		return nil, nil
	}

	eventVal := reflect.Indirect(reflect.ValueOf(event))
	encodedAddons := make([]queuedproto.PerlAddonData, 0, len(supportedAddons.addonFields))

	for _, addon := range supportedAddons.addonFields {
		// нужен только маршалер, поэтому приводим к сокращённому интерфейсу, без получения Addr(),
		// которого может не быть в случае, если event - не указатель
		data, err := eventVal.Field(addon.fieldIdx).Interface().(addonMarshaler).MarshalAddon()
		if err != nil {
			return nil, fmt.Errorf("%T: %w: AddonID=%d", event, err, addon.addonID)
		}

		encodedAddons = append(encodedAddons, queuedproto.PerlAddonData{
			ID:   addon.addonID,
			Data: data,
		})
	}

	return encodedAddons, nil
}

// getEventTypeAddons получает список аддонов, поддерживаемой структурой события.
// данные для каждого типа кешируются.
//
// TODO защиты от cache stampede нет - учитывая, что кешируем навсегда, и вычисление списка быстрое (структуры по 5-20 полей), наверноe, оно не нужно.
func getEventTypeAddons(event interface{}) (supportedAddons *eventTypeAddons) {
	typ := reflect.TypeOf(event)

	if cachedI, ok := eventTypeAddonCache.Load(typ); ok {
		return cachedI.(*eventTypeAddons)
	}

	// slow path

	defer func() {
		eventTypeAddonCache.Store(typ, supportedAddons)
		eventTypeAddonCache.Store(reflect.PtrTo(typ), supportedAddons)
	}()

	val := reflect.ValueOf(event) // используется только для вызова метода AddonID(), по данным не лазим

	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
		val = val.Elem()
	}

	if typ.Kind() != reflect.Struct {
		return nil
	}

	numFields := typ.NumField()
	for i := 0; i < numFields; i++ {
		//if !typ.Field(i).IsExported() { // TODO go1.17+ - после апгрейда версии заменить на более понятный вызов
		if typ.Field(i).PkgPath != "" {
			continue
		}

		field := val.Field(i)

		// см. длинный коммент перед type addonMarshaler
		if field.CanAddr() {
			field = field.Addr() // скорее всего, имеем дело с декодером. либо - с энкодером по указателю
		} else {
			/*
				Ещё более жёсткий хак, чем сама идея.
				Приведение к addons.PerlAddon требуется лишь для проверки соответствия интерфейсу (маршалеры вызываются
				в других функциях), и для получения id методом AddonID.

				Предполагается, что этот метод данными не пользуется, иначе это какой-то странный метод.
				Поэтому можно ничего не копировать, а тупо создать пустое поле.

				Вообще, если мы попали сюда - значит имеем дело с энкодером по значению. Т.е. данные в структуре всё-таки
				есть (в отл. от декодера).

				Но опять же - если AddonID опирается на данные внутри структуры - это ошибка проектирования.
				При декодировании события в tp.eventListFromItems создаётся пустая структура, и данных там точно не будет.
				Поэтому, пусть лучше вероятность наступления на эти грабли будет выше ))
				Прогнозировать, что случится первым - публикация такого события, или его приход из очередь, невозможно.

				Если это зачем-то(!) потребовалось - лучше подумать про рефакторинг, а не городить костыли, и
				раскомментировать "честный" вариант с копированием:

				fieldCopyPtr := reflect.New(field.Type())
				fieldCopyPtr.Elem().Set(field)
				field = fieldCopyPtr
			*/
			field = reflect.New(field.Type())
		}

		addon, ok := field.Interface().(addons.PerlAddon)
		if !ok {
			continue
		}

		if supportedAddons == nil {
			supportedAddons = &eventTypeAddons{
				addonMap:    make(addonMap, int(addons.MaxAddonID)),
				addonFields: make([]addonField, 0, addons.MaxAddonID),
			}
		}

		addonID := addon.AddonID()
		supportedAddons.addonMap[addonID] = i
		supportedAddons.addonFields = append(supportedAddons.addonFields, addonField{
			addonID:  addonID,
			fieldIdx: i,
		})
	}

	return supportedAddons
}

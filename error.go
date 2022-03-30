package tp

import "errors"

var (
	// ErrUnknownType - тип данных не зарегистрирован методом Register
	ErrUnknownType = errors.New("unregistered type")

	// ErrUnknownQueue - очередь не зарегистрирована методом Register
	ErrUnknownQueue = errors.New("unregistered queue")

	// ErrVersionTooBig - версия не должна превышать MaxVersion
	ErrVersionTooBig = errors.New("version number exceeds MaxVersion")

	// ErrInternalHandler - тип структуры юзерского события должен поддерживать Handler, т.к. Register принимает Handler. Говорит о серьёзной ошибке внутри tp
	ErrInternalHandler = errors.New("internal error: handlerInfo.dataType is not Handler")

	// ErrHardMemoryLimit - достигнут лимит по памяти oom_hard_limit
	ErrHardMemoryLimit = errors.New("memory usage is over the hard limit, stopping processing")

	// ErrPointerRequired - методу tp.Next требуется указатель на ресивер метода Handler (т.е. указатель на указатель на объект юзерского события)
	ErrPointerRequired = errors.New("tp.Next: pointer to Handle method's receiver is required")

	// ErrEventTypesDiffer - в цепочке событий типы различаются (скорее всего, внутренняя ошибка в eventListFromItems)
	ErrEventTypesDiffer = errors.New("tp.Next: event types differ")

	// ErrMisconstructedEvent - не надо инициализировать внутренности событий вручную.
	// Скорее всего, установили next = (*MyEvent)(nil) вместо Handler(nil)
	ErrMisconstructedEvent = errors.New("tp.Next: misconstructed event")

	// ErrStructRequired - для использования автозаполнения, тип события должен быть структурой/указателем на структуру
	ErrStructRequired = errors.New("struct type is required for autofill feature")

	// ErrExportedRequired - для использования автозаполнения требуется экспортируемое (публичное) поле
	ErrExportedRequired = errors.New("exported (public) field is required for autofill feature")

	// ErrFieldNotFound - не найдено поле с типом аргумента autofill
	ErrFieldNotFound = errors.New("no field found")
)

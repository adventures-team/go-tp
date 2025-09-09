package queueconfig

import (
	"errors"
	"fmt"

	"github.com/rs/zerolog"
)

const separator = ": "

// Ошибки в конфиге
var (
	ErrNotFound             = errors.New("path not found")
	ErrOOMSoftDownscaleRate = errors.New("oom_soft_downscale_rate should be >= 0")
	ErrWorkersCount         = errors.New("workers count should be >=0")
	ErrSlotsCount           = errors.New("slots count should be >=0")
)

// ConfError - тип для ошибок, который можно как вернуть юзеру - так и эффективно записать в лог.
// Преобразование в строку делается, только если это потребовано явно (вызовом метода Error), а не по факту возврата ошибки (как это обычно принято).
//
// При выводе в zerolog методом Err, поля будут напечатаны отдельно (без преобразование в одну строку методом Error).
type ConfError struct {
	Path       string      // Путь к неправильному значению
	Value      string      // Ошибочное значение. Если параметр не найден - пустое
	Err        error       // Ошибка парзинга, либо ErrNotFound для ненайденных значений
	Default    interface{} // Значение по умолчанию
	isNotFound bool        // Err = ErrNotFound, для упрощения проверок в логике конфига
}

func errNotFound(path string) *ConfError {
	return &ConfError{
		Path:       path,
		Err:        ErrNotFound,
		isNotFound: true,
	}
}

func isNotFound(err error) bool {
	confErr, ok := err.(*ConfError)
	return ok && confErr.isNotFound
}

var (
	_ error                      = &ConfError{}
	_ zerolog.LogObjectMarshaler = &ConfError{}
)

// Unwrap возвращает Err для работы с errors.Is и т.д.
func (ce *ConfError) Unwrap() error {
	return ce.Err
}

// Error преобразует ошибку в строку. Value пишется, если ошибка - не ErrNotFound, даже если значение пустое
// (чтобы сообщить о том, что пустое значение для данного параметра не является валидным).
func (ce *ConfError) Error() string {
	val := ""
	if !ce.isNotFound {
		val = ce.Value + separator
	}

	if ce.Default != nil {
		return fmt.Sprintf("%s: %s%v, using default %v", ce.Path, val, ce.Err, ce.Default)
	}

	return ce.Path + separator + val + ce.Err.Error()
}

// MarshalZerologObject печатает ошибку в лог.
// Используется при печати ошибки методами zerolog.Event.Object и zerolog.Event.Err.
//
// Поля:
//
//	conf_path: путь в онлайнконфе
//	err_value: если путь найден - ошибочное значение
//	err:       ошибка ("path not found", если путь не найден, либо ошибка парзинга)
//	default:   если есть значение по умолчанию
func (ce *ConfError) MarshalZerologObject(ev *zerolog.Event) {
	ev = ev.Str("conf_path", ce.Path)

	if !ce.isNotFound {
		ev = ev.Str("err_value", ce.Value)
	}

	ev = ev.Err(ce.Err)

	if ce.Default != nil {
		ev.Interface("default", ce.Default)
	}
}

/* см. комменты в getStringPerQueue и getIntPerQueue, выпилить, если не пригодится.
func isNotFound(err error) bool {
	if cErr, ok := err.(*ConfError); ok {
		return cErr.isNotFound
	}

	return false
}
*/

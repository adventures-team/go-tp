//nolint:gosec,revive
package tp

// ТУТ АДЪ

import (
	"reflect"
	"unsafe"

	"github.com/rs/zerolog"
	zLog "github.com/rs/zerolog/log"
)

// EnableUnsafeLogLevel - разрешить установку уровня логгирования при помощи unsafe.
//
// zerolog.SetGlobalLevel устанавливается независимо от этого флага.
//
// Отключить, если будет пугать race detector. go:norace не работает, т.к. из-за unsafe не может определить виновную горутину/функцию.
//
// TODO отрефакторить логгирование, чтобы не требовался этот костыль.
var EnableUnsafeLogLevel = true

// очень костыльная реализация. "идиоматичная" схема (zerolog.Logger.Level) не очень ложится в архитектуру,
// а метод для inplace-установки они не предусмотрели. и зря:
// 1. можно было сделать атомарно (т.к. идиоматика "копирование вместо шаринга" делается ради отказа от локов, а не чтобы напрягать разработчиков!)
// 2. запись/чтение int8 атомарны почти везде, но могут быть reorder-спецэффекты. для данной задачи и то, и другое простительно - апдейтер онлайнконфа и так даёт задержку до 30 сек)
// пришлось извращаться с ручным поиском поля, т.к. reflect.Value.FieldByName не ищет приватные поля

var levelOffset = func() uintptr {
	typ := reflect.TypeOf(zLog.Logger)

	for i := 0; i < typ.NumField(); i++ {
		if field := typ.Field(i); field.Name == "level" {
			return field.Offset
		}
	}

	zLog.Fatal().Msg("no zerolog.Logger.level field found")

	return 0 // unreachable
}()

func setLogLevel(logger *zerolog.Logger, newLevel zerolog.Level) {
	zerolog.SetGlobalLevel(newLevel)

	if EnableUnsafeLogLevel {
		*(*zerolog.Level)(unsafe.Add(unsafe.Pointer(logger), levelOffset)) = newLevel
	}
}

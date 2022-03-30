package encodings

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// Format - тип ID формата сообщения. Все форматы должны начинаться с uint8 с номером версии.
type Format uint

const (
	Perl Format = iota
	Protobuf
)

type (
	encoder func(version uint8, val interface{}) ([]byte, error)
	decoder func(encoded []byte, valPtr interface{}) error
)

var (
	encoders = [...]encoder{
		perlEncoder,
		protobufEncoder,
	}

	decoders = [...]decoder{
		perlDecoder,
		protobufDecoder,
	}
)

// Encode кодирует данные val в указанном формате и версии. val может быть как значением, так и указателем.
func Encode(format Format, version uint8, val interface{}) ([]byte, error) {
	if format >= Format(len(encoders)) {
		return nil, fmt.Errorf("%w: %d", ErrInvalidFormat, format)
	}

	return encoders[format](version, val)
}

// Decode декодирует encoded в valPtr, который должен быть указателем.
//
// TODO проверить хотя бы в API верхнего уровня, что не будет паники, если передать по значению.
// Когда устаканятся дженерики - потребовать указатель на этапе компиляции.
func Decode(format Format, encoded []byte, valPtr interface{}) error {
	if format >= Format(len(decoders)) {
		return fmt.Errorf("%w: %d", ErrInvalidFormat, format)
	}

	return decoders[format](encoded, valPtr)
}

// Detect определяет формат структуры данных по соответствию интерфейсу - proto.Message для protobuf, всё остальное считается перловым форматом
func Detect(v interface{}) Format {
	switch v.(type) {
	case proto.Message:
		return Protobuf

	default:
		return Perl
	}
}

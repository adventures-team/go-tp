package encodings

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/adventures-team/go-iproto"
)

func protobufEncoder(version uint8, valPtr iproto.Marshaler) ([]byte, error) {
	msg, ok := valPtr.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%w: %T", ErrProtobuf, valPtr)
	}

	encoded, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	ret := make([]byte, len(encoded)+1)
	copy(ret[1:], encoded)
	ret[0] = version

	return ret, nil
}

func protobufDecoder(encoded []byte, valPtr iproto.Unmarshaler) error {
	msg, ok := valPtr.(proto.Message)
	if !ok {
		return fmt.Errorf("%w: %T", ErrProtobuf, valPtr)
	}

	return proto.Unmarshal(encoded[1:], msg)
}

package encodings

import (
	"github.com/my-mail-ru/go-iproto"

	"github.com/my-mail-ru/go-queuedproto"

	"gitlab.cebc.tools/go/tp.git/internal"
)

func perlEncoder(version uint8, valPtr iproto.Marshaler) ([]byte, error) {
	addons, err := encodeAddonsData(valPtr)
	if err != nil {
		return nil, err
	}

	data, err := valPtr.MarshalIProto(internal.AllocBytes())
	if err != nil {
		return nil, err
	}

	return queuedproto.PerlData{
		Version: version,
		Addons:  queuedproto.PerlAddons{List: addons},
		Data:    data,
	}.MarshalIProto(internal.AllocBytes())
}

func perlDecoder(encoded []byte, valPtr iproto.Unmarshaler) error {
	var perlData queuedproto.PerlData

	if _, err := perlData.UnmarshalIProto(encoded); err != nil {
		return err
	}

	if len(perlData.Addons.List) != 0 {
		if err := decodeAddonsData(valPtr, perlData.Addons.List); err != nil {
			return err
		}
	}

	_, err := valPtr.UnmarshalIProto(perlData.Data)

	return err
}

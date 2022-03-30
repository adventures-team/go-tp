package encodings

import (
	"github.com/funny-falcon/go-iproto/marshal"

	"gitlab.corp.mail.ru/mygo/queuedproto.git"
)

func perlEncoder(version uint8, valPtr interface{}) ([]byte, error) {
	addons, err := encodeAddonsData(valPtr)
	if err != nil {
		return nil, err
	}

	return marshal.Write(queuedproto.PerlData{
		Version: version,
		Addons:  queuedproto.PerlAddons{List: addons},
		Data:    marshal.Write(valPtr),
	}), nil
}

func perlDecoder(encoded []byte, valPtr interface{}) error {
	var perlData queuedproto.PerlData

	if err := marshal.Read(encoded, &perlData); err != nil {
		return err
	}

	if len(perlData.Addons.List) != 0 {
		if err := decodeAddonsData(valPtr, perlData.Addons.List); err != nil {
			return err
		}
	}

	return marshal.Read(perlData.Data, valPtr)
}

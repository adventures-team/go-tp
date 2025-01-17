package encodings

import (
	"gitlab.corp.my.games/adventures/go/easygo.git/iproto"
	"gitlab.corp.my.games/adventures/go/queuedproto.git"
)

func perlEncoder(version uint8, valPtr interface{}) ([]byte, error) {
	addons, err := encodeAddonsData(valPtr)
	if err != nil {
		return nil, err
	}

	data, err := iproto.Pack(valPtr)
	if err != nil {
		return nil, err
	}

	return iproto.Pack(queuedproto.PerlData{
		Version: version,
		Addons:  queuedproto.PerlAddons{List: addons},
		Data:    data,
	})
}

func perlDecoder(encoded []byte, valPtr interface{}) error {
	var perlData queuedproto.PerlData

	if _, err := iproto.Unpack(encoded, &perlData); err != nil {
		return err
	}

	if len(perlData.Addons.List) != 0 {
		if err := decodeAddonsData(valPtr, perlData.Addons.List); err != nil {
			return err
		}
	}

	_, err := iproto.Unpack(perlData.Data, valPtr)

	return err
}

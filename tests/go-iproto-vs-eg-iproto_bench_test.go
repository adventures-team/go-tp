//go:build !integration && !gomarkdoc
// +build !integration,!gomarkdoc

package tptest

import (
	"reflect"
	"testing"

	"github.com/funny-falcon/go-iproto/marshal"

	"gitlab.corp.mail.ru/e-mail-ru/easygo/iproto"
)

const checkResult = false

// для честного сравнения, типы сделаны совместимыми (с uint32-длинами), без кастомных маршалеров

type (
	perlData struct {
		Version    uint8
		Addons     perlAddons
		Extensions perlExtensions
		Data       []byte
	}

	perlAddonData struct {
		ID   uint16
		Data []byte
	}

	perlAddons struct {
		List []perlAddonData
	}

	perlExtensions struct {
		Flags uint32
		Data  []byte
	}
)

var iprotoData = perlData{
	Version: 8,
	Addons: perlAddons{
		List: []perlAddonData{
			{22, []byte("foobar")},
			{33, []byte("zxcv")},
			{44, []byte("go iproto test")},
		},
	},
	Extensions: perlExtensions{
		Flags: 123,
		Data:  []byte("test"),
	},
	Data: []byte("test data asdfghjkqwertyuio foobar"),
}

func BenchmarkGOIprotoMarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		marshal.Write(iprotoData)
	}
}

func BenchmarkGOIprotoUnmarshal(b *testing.B) {
	encoded := marshal.Write(iprotoData)
	decoded := perlData{}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		marshal.Read(encoded, &decoded) //nolint:errcheck

		if checkResult {
			if !reflect.DeepEqual(decoded, iprotoData) {
				b.Fatalf("got: %#v, expected: %#v", decoded, iprotoData)
			}
		}
	}
}

func BenchmarkEGIprotoMarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		iproto.Pack(iprotoData) //nolint:errcheck
	}
}

func BenchmarkEGIprotoUnmarshal(b *testing.B) {
	decoded := perlData{}
	encoded, err := iproto.Pack(iprotoData)

	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		iproto.Unpack(encoded, &decoded) //nolint:errcheck

		if checkResult {
			if !reflect.DeepEqual(decoded, iprotoData) {
				b.Fatalf("got: %#v, expected: %#v", decoded, iprotoData)
			}
		}
	}
}

//go:build !integration
// +build !integration

package encodings

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/my-mail-ru/go-queuedproto"
	"github.com/my-mail-ru/go-queuedproto/addons"
)

// см. queuedproto/perl_format_test.go
var testPerlBytes = []byte{
	0x62, 0x02, 0x01, 0x00, 0x04, 0xA1, 0xE0, 0x14, 0x62, 0x04, 0x00, 0x10,
	0x0F, 0x55, 0x4E, 0x4B, 0x4E, 0x4F, 0x57, 0x4E, 0x2E, 0x55, 0x4E, 0x4B, 0x4E, 0x4F, 0x57, 0x4E,
	0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
}

//adv:iproto:
type testPerlData struct {
	addons.CreationTime
	addons.Producer

	ID    uint32
	Value uint32
}

func TestPerlDecode(t *testing.T) {
	var event testPerlData

	if err := Decode(Perl, testPerlBytes, &event); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, uint32(1), event.ID, "ID doesn't match")
	assert.Equal(t, uint32(2), event.Value, "Value doesn't match")
	assert.Greater(t, event.CreationTime, uint32(0), "CreationTime must be set")
	assert.Equal(t, "UNKNOWN.UNKNOWN", string(event.Producer), "Producer doesn't match")
}

func TestPerlEncode(t *testing.T) {
	var event testPerlData

	if err := Decode(Perl, testPerlBytes, &event); err != nil {
		t.Fatal("decoding perl addons failed:", err)
	}

	version := testPerlBytes[0] & queuedproto.VersionMask

	encoded, err := Encode(Perl, version, event)
	if err != nil {
		t.Fatal("encoding perl addons by value failed:", err)
	}

	assert.Equal(t, testPerlBytes, encoded, "original and reencoded (by value) data differ")

	encoded, err = Encode(Perl, version, &event)
	if err != nil {
		t.Fatal("encoding perl addons by pointer failed:", err)
	}

	assert.Equal(t, testPerlBytes, encoded, "original and reencoded (by pointer) data differ")
}

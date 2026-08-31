package protocol

import (
	"bytes"
	"testing"
)

func FuzzReadFrame(f *testing.F) {
	f.Add([]byte{0, 0, 0, 2, '{', '}'})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ReadFrame(bytes.NewReader(data))
	})
}

func FuzzDecodeStrict(f *testing.F) {
	f.Add([]byte(`{"kind":"hello","payload":{}}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		var dst Envelope
		_ = DecodeStrict(data, &dst)
	})
}

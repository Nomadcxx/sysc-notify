package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"
)

func TestFrameRoundTripWithFragmentedReads(t *testing.T) {
	var wire bytes.Buffer
	want := []byte(`{"kind":"hello","payload":{}}`)
	if err := WriteFrame(&wire, want); err != nil {
		t.Fatal(err)
	}

	got, err := ReadFrame(&chunkReader{r: bytes.NewReader(wire.Bytes()), n: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("ReadFrame() = %q, want %q", got, want)
	}
}

func TestReadFrameLeavesFollowingFrame(t *testing.T) {
	var wire bytes.Buffer
	for _, payload := range [][]byte{[]byte(`{"kind":"one"}`), []byte(`{"kind":"two"}`)} {
		if err := WriteFrame(&wire, payload); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{`{"kind":"one"}`, `{"kind":"two"}`} {
		got, err := ReadFrame(&wire)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != want {
			t.Fatalf("ReadFrame() = %q, want %q", got, want)
		}
	}
}

func TestFrameRejectsInvalidLengths(t *testing.T) {
	for _, tc := range []struct {
		name string
		n    uint32
	}{
		{name: "zero", n: 0},
		{name: "over limit", n: MaxFrameSize + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var header [4]byte
			binary.BigEndian.PutUint32(header[:], tc.n)
			if _, err := ReadFrame(bytes.NewReader(header[:])); err == nil {
				t.Fatal("ReadFrame() accepted invalid length")
			}
		})
	}
	if err := WriteFrame(io.Discard, nil); err == nil {
		t.Fatal("WriteFrame() accepted empty payload")
	}
	if err := WriteFrame(io.Discard, make([]byte, MaxFrameSize+1)); err == nil {
		t.Fatal("WriteFrame() accepted oversized payload")
	}
}

func TestReadFrameRejectsTruncation(t *testing.T) {
	var wire bytes.Buffer
	if err := WriteFrame(&wire, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	data := wire.Bytes()
	if _, err := ReadFrame(bytes.NewReader(data[:len(data)-1])); err == nil {
		t.Fatal("ReadFrame() accepted truncated payload")
	}
}

func TestDecodeStrictRejectsAmbiguousJSON(t *testing.T) {
	tests := []string{
		`{"kind":"hello"} {"kind":"hello"}`,
		`{"kind":"hello","kind":"snapshot","payload":{}}`,
		`{"kind":"hello","payload":{"major":1,"major":2}}`,
		`{"kind":"hello","payload":{},"unexpected":true}`,
	}
	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			var dst Envelope
			if err := DecodeStrict([]byte(input), &dst); err == nil {
				t.Fatal("DecodeStrict() accepted invalid JSON")
			}
		})
	}
}

func TestDecodeStrictAcceptsOneObject(t *testing.T) {
	var dst Envelope
	if err := DecodeStrict([]byte(`{"kind":"hello","payload":{}}`), &dst); err != nil {
		t.Fatal(err)
	}
	if dst.Kind != "hello" {
		t.Fatalf("Kind = %q, want hello", dst.Kind)
	}
}

type chunkReader struct {
	r io.Reader
	n int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if len(p) > r.n {
		p = p[:r.n]
	}
	return r.r.Read(p)
}

func TestDecodeStrictRejectsInvalidUTF8(t *testing.T) {
	input := append([]byte(`{"kind":"`), 0xff)
	input = append(input, []byte(`","payload":{}}`)...)
	var dst Envelope
	if err := DecodeStrict(input, &dst); err == nil || !strings.Contains(err.Error(), "UTF-8") {
		t.Fatalf("DecodeStrict() error = %v, want UTF-8 error", err)
	}
}

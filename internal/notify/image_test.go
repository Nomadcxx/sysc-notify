package notify

import (
	"bytes"
	"image/png"
	"math"
	"testing"

	"github.com/Nomadcxx/sysc-notify/protocol"
)

func TestNormalizeImageDownscalesToWireBounds(t *testing.T) {
	const width, height = 1024, 2
	data := make([]byte, width*height*4)
	for i := range data {
		data[i] = byte(i)
	}
	got, err := normalizeImage(RawImage{
		Width: width, Height: height, RowStride: width * 4,
		HasAlpha: true, BitsPerSample: 8, Channels: 4, Data: data,
	})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := png.Decode(bytes.NewReader(got.Data))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Bounds().Dx() != protocol.MaxWireImageLongEdge || decoded.Bounds().Dy() != 1 {
		t.Fatalf("decoded bounds = %v", decoded.Bounds())
	}
	if len(got.Data) > protocol.MaxWireImageBytes {
		t.Fatalf("encoded image is %d bytes", len(got.Data))
	}
}

func TestNormalizeTreatsMalformedOptionalImageAsTextOnly(t *testing.T) {
	request := Request{
		Summary: "still shown",
		Hints: map[string]any{HintImageData: RawImage{
			Width: 1, Height: 1, RowStride: 3, BitsPerSample: 16, Channels: 3, Data: []byte{0, 0, 0},
		}},
	}
	got, err := Normalize(request)
	if err != nil {
		t.Fatal(err)
	}
	if got.Image != nil || !got.ImageRejected {
		t.Fatalf("image result = %#v", got)
	}
}

func TestRawImageValidationRejectsUnsafeMetadata(t *testing.T) {
	tests := map[string]RawImage{
		"zero dimension": {Height: 1, RowStride: 3, BitsPerSample: 8, Channels: 3, Data: []byte{0, 0, 0}},
		"wide":           {Width: protocol.MaxSourceImageDimension + 1, Height: 1, RowStride: math.MaxInt32, BitsPerSample: 8, Channels: 4, Data: []byte{0}},
		"bad channels":   {Width: 1, Height: 1, RowStride: 2, BitsPerSample: 8, Channels: 2, Data: []byte{0, 0}},
		"bad bits":       {Width: 1, Height: 1, RowStride: 3, BitsPerSample: 16, Channels: 3, Data: []byte{0, 0, 0}},
		"short stride":   {Width: math.MaxInt32, Height: math.MaxInt32, RowStride: 1, BitsPerSample: 8, Channels: 4, Data: []byte{0}},
		"short data":     {Width: 2, Height: 2, RowStride: 6, BitsPerSample: 8, Channels: 3, Data: []byte{0}},
		"large data":     {Width: 1, Height: 1, RowStride: 3, BitsPerSample: 8, Channels: 3, Data: make([]byte, protocol.MaxSourceImageBytes+1)},
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if err := raw.Validate(); err == nil {
				t.Fatal("Validate() accepted invalid image")
			}
		})
	}
}

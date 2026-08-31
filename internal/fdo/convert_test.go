package fdo

import (
	"testing"

	"github.com/godbus/dbus/v5"

	"github.com/Nomadcxx/sysc-notify/internal/notify"
)

func TestConvertHintsDecodesImageStruct(t *testing.T) {
	pixels := []byte{
		0xff, 0x00, 0x00, 0xff,
		0x00, 0xff, 0x00, 0xff,
	}
	hints, err := convertHints(map[string]dbus.Variant{
		notify.HintUrgency:   dbus.MakeVariant(byte(2)),
		notify.HintImageData: dbus.MakeVariant(imageData{Width: 2, Height: 1, RowStride: 8, HasAlpha: true, BitsPerSample: 8, Channels: 4, Data: pixels}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if hints[notify.HintUrgency] != byte(2) {
		t.Fatalf("urgency = %#v", hints[notify.HintUrgency])
	}
	image, ok := hints[notify.HintImageData].(notify.RawImage)
	if !ok || image.Width != 2 || image.Height != 1 || len(image.Data) != len(pixels) {
		t.Fatalf("image hint = %#v", hints[notify.HintImageData])
	}
}

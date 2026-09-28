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

func TestConvertHintsAcceptsLegacyImageDataNames(t *testing.T) {
	pixels := []byte{0xff, 0x00, 0x00, 0xff}
	for _, key := range []string{"image_data", "icon_data"} {
		t.Run(key, func(t *testing.T) {
			hints, err := convertHints(map[string]dbus.Variant{
				key: dbus.MakeVariant(imageData{Width: 1, Height: 1, RowStride: 4, HasAlpha: true, BitsPerSample: 8, Channels: 4, Data: pixels}),
			})
			if err != nil {
				t.Fatal(err)
			}
			image, ok := hints[notify.HintImageData].(notify.RawImage)
			if !ok || image.Width != 1 || image.Height != 1 {
				t.Fatalf("image hint = %#v", hints[notify.HintImageData])
			}
		})
	}
}

func TestConvertHintsPrefersCanonicalImageData(t *testing.T) {
	variant := func(width int32) dbus.Variant {
		return dbus.MakeVariant(imageData{Width: width, Height: 1, RowStride: width * 4, HasAlpha: true, BitsPerSample: 8, Channels: 4, Data: make([]byte, width*4)})
	}
	hints, err := convertHints(map[string]dbus.Variant{
		"image_data":         variant(1),
		"icon_data":          variant(3),
		notify.HintImageData: variant(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	image, ok := hints[notify.HintImageData].(notify.RawImage)
	if !ok || image.Width != 2 {
		t.Fatalf("preferred image hint = %#v, want width 2", hints[notify.HintImageData])
	}
}

func TestConvertHintsDropsMalformedImageHints(t *testing.T) {
	for name, hints := range map[string]map[string]dbus.Variant{
		"image-path":      {notify.HintImagePath: dbus.MakeVariant(int32(1))},
		"image-data":      {notify.HintImageData: dbus.MakeVariant([]any{int32(1), int32(1)})},
		"image-data type": {notify.HintImageData: dbus.MakeVariant("not an image")},
	} {
		t.Run(name, func(t *testing.T) {
			hints[notify.HintUrgency] = dbus.MakeVariant(byte(2))
			got, err := convertHints(hints)
			if err != nil {
				t.Fatalf("convertHints() = %v, want the malformed image dropped", err)
			}
			if _, ok := got[notify.HintImagePath]; ok {
				t.Fatalf("hints = %#v, kept malformed image-path", got)
			}
			if _, ok := got[notify.HintImageData]; ok {
				t.Fatalf("hints = %#v, kept malformed image-data", got)
			}
			if got[notify.HintUrgency] != byte(2) {
				t.Fatalf("hints = %#v, lost urgency", got)
			}
		})
	}
}

package fdo

import (
	"errors"
	"fmt"

	"github.com/godbus/dbus/v5"

	"github.com/Nomadcxx/sysc-notify/internal/notify"
	"github.com/Nomadcxx/sysc-notify/protocol"
)

type imageData struct {
	Width         int32
	Height        int32
	RowStride     int32
	HasAlpha      bool
	BitsPerSample int32
	Channels      int32
	Data          []byte
}

func convertHints(source map[string]dbus.Variant) (map[string]any, error) {
	if len(source) > protocol.MaxHints {
		return nil, errors.New("fdo: too many hints")
	}
	result := make(map[string]any, len(source))
	for key, variant := range source {
		switch key {
		case notify.HintUrgency, notify.HintTransient, notify.HintPrivate, notify.HintResident,
			notify.HintDesktopEntry, notify.HintCategory, notify.HintValue, notify.HintInlineReplyPlaceholder:
			result[key] = variant.Value()
		case notify.HintImageData:
			image, err := convertImageData(variant.Value())
			if err != nil {
				return nil, err
			}
			result[key] = image
		}
	}
	return result, nil
}

func convertImageData(value any) (notify.RawImage, error) {
	if data, ok := value.(imageData); ok {
		return notify.RawImage{
			Width: data.Width, Height: data.Height, RowStride: data.RowStride, HasAlpha: data.HasAlpha,
			BitsPerSample: data.BitsPerSample, Channels: data.Channels, Data: data.Data,
		}, nil
	}
	fields, ok := value.([]any)
	if !ok || len(fields) != 7 {
		return notify.RawImage{}, errors.New("fdo: malformed image-data hint")
	}
	width, widthOK := fields[0].(int32)
	height, heightOK := fields[1].(int32)
	rowStride, strideOK := fields[2].(int32)
	hasAlpha, alphaOK := fields[3].(bool)
	bits, bitsOK := fields[4].(int32)
	channels, channelsOK := fields[5].(int32)
	data, dataOK := fields[6].([]byte)
	if !widthOK || !heightOK || !strideOK || !alphaOK || !bitsOK || !channelsOK || !dataOK {
		return notify.RawImage{}, fmt.Errorf("fdo: malformed image-data fields")
	}
	return notify.RawImage{
		Width: width, Height: height, RowStride: rowStride, HasAlpha: hasAlpha,
		BitsPerSample: bits, Channels: channels, Data: data,
	}, nil
}

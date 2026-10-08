package notify

import (
	"bytes"
	"errors"
	"image"
	"image/png"

	"github.com/Nomadcxx/sysc-notify/protocol"
)

type RawImage struct {
	Width, Height int32
	RowStride     int32
	HasAlpha      bool
	BitsPerSample int32
	Channels      int32
	Data          []byte
}

func (r RawImage) Validate() error {
	if r.Width <= 0 || r.Height <= 0 || r.Width > protocol.MaxSourceImageDimension || r.Height > protocol.MaxSourceImageDimension {
		return errors.New("notify: invalid image dimensions")
	}
	if r.BitsPerSample != 8 || (r.Channels != 3 && r.Channels != 4) || (r.HasAlpha && r.Channels != 4) {
		return errors.New("notify: unsupported image format")
	}
	minimumStride := int64(r.Width) * int64(r.Channels)
	if r.RowStride <= 0 || int64(r.RowStride) < minimumStride {
		return errors.New("notify: invalid image row stride")
	}
	rawBytes := int64(r.RowStride) * int64(r.Height)
	// The last row needs no padding: libnotify sends
	// (height-1)*rowstride + width*channels, while GdkPixbuf-style senders may
	// pad it out to rowstride*height.
	minimumBytes := int64(r.RowStride)*int64(r.Height-1) + minimumStride
	decodedBytes := int64(r.Width) * int64(r.Height) * 4
	dataBytes := int64(len(r.Data))
	if rawBytes > protocol.MaxSourceImageBytes || decodedBytes > protocol.MaxSourceImageBytes ||
		dataBytes < minimumBytes || dataBytes > rawBytes {
		return errors.New("notify: invalid image data length")
	}
	return nil
}

func normalizeImage(raw RawImage) (*protocol.Image, error) {
	if err := raw.Validate(); err != nil {
		return nil, err
	}
	width, height := int(raw.Width), int(raw.Height)
	dstWidth, dstHeight := width, height
	if longest := max(width, height); longest > protocol.MaxWireImageLongEdge {
		dstWidth = max(1, width*protocol.MaxWireImageLongEdge/longest)
		dstHeight = max(1, height*protocol.MaxWireImageLongEdge/longest)
	}
	dst := image.NewNRGBA(image.Rect(0, 0, dstWidth, dstHeight))
	for y := 0; y < dstHeight; y++ {
		sourceY := y * height / dstHeight
		for x := 0; x < dstWidth; x++ {
			sourceX := x * width / dstWidth
			source := sourceY*int(raw.RowStride) + sourceX*int(raw.Channels)
			// The highest index read is (height-1)*rowstride + width*channels - 1,
			// which is within the minimum length Validate accepts, padding or not.
			target := y*dst.Stride + x*4
			dst.Pix[target], dst.Pix[target+1], dst.Pix[target+2] = raw.Data[source], raw.Data[source+1], raw.Data[source+2]
			dst.Pix[target+3] = 0xff
			if raw.HasAlpha {
				dst.Pix[target+3] = raw.Data[source+3]
			}
		}
	}
	var encoded bytes.Buffer
	if err := png.Encode(&encoded, dst); err != nil {
		return nil, err
	}
	if encoded.Len() > protocol.MaxWireImageBytes {
		return nil, errors.New("notify: encoded image exceeds wire limit")
	}
	return &protocol.Image{
		MediaType: "image/png", Width: uint32(dstWidth), Height: uint32(dstHeight), Data: encoded.Bytes(),
	}, nil
}

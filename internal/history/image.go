package history

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"image"
	"image/png"
	"os"
	"path/filepath"

	"github.com/Nomadcxx/sysc-notify/protocol"
)

const historyImageLongEdge = 96

func normalizeHistoryImage(source *protocol.Image) (*protocol.Image, error) {
	if source == nil {
		return nil, nil
	}
	if err := source.Validate(); err != nil {
		return nil, err
	}
	decoded, err := png.Decode(bytes.NewReader(source.Data))
	if err != nil {
		return nil, fmt.Errorf("history: decode image: %w", err)
	}
	bounds := decoded.Bounds()
	if bounds.Dx() != int(source.Width) || bounds.Dy() != int(source.Height) {
		return nil, errors.New("history: image dimensions do not match PNG")
	}
	width, height := bounds.Dx(), bounds.Dy()
	if longest := max(width, height); longest > historyImageLongEdge {
		width = max(1, width*historyImageLongEdge/longest)
		height = max(1, height*historyImageLongEdge/longest)
	}
	destination := image.NewNRGBA(image.Rect(0, 0, width, height))
	for y := range height {
		sourceY := bounds.Min.Y + y*bounds.Dy()/height
		for x := range width {
			sourceX := bounds.Min.X + x*bounds.Dx()/width
			destination.Set(x, y, decoded.At(sourceX, sourceY))
		}
	}
	var encoded bytes.Buffer
	if err := png.Encode(&encoded, destination); err != nil {
		return nil, fmt.Errorf("history: encode image: %w", err)
	}
	return &protocol.Image{
		MediaType: "image/png", Width: uint32(width), Height: uint32(height), Data: encoded.Bytes(),
	}, nil
}

func imageReference(source *protocol.Image) *diskImage {
	if source == nil {
		return nil
	}
	digest := sha256.Sum256(source.Data)
	return &diskImage{SHA256: hex.EncodeToString(digest[:]), Width: source.Width, Height: source.Height}
}

func loadImage(dir string, reference *diskImage) (*protocol.Image, error) {
	if reference == nil {
		return nil, nil
	}
	if len(reference.SHA256) != sha256.Size*2 || reference.Width == 0 || reference.Height == 0 ||
		reference.Width > historyImageLongEdge || reference.Height > historyImageLongEdge {
		return nil, errors.New("invalid image reference")
	}
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil || hex.EncodeToString(digest) != reference.SHA256 {
		return nil, errors.New("invalid image digest")
	}
	path := filepath.Join(dir, reference.SHA256+".png")
	contents, err := readPrivateRegularFile(path, protocol.MaxWireImageBytes)
	if err != nil {
		return nil, err
	}
	actual := sha256.Sum256(contents)
	if !bytes.Equal(actual[:], digest) {
		return nil, errors.New("image digest mismatch")
	}
	result := &protocol.Image{MediaType: "image/png", Width: reference.Width, Height: reference.Height, Data: contents}
	decoded, err := png.Decode(bytes.NewReader(contents))
	if err != nil {
		return nil, fmt.Errorf("decode sidecar: %w", err)
	}
	if bounds := decoded.Bounds(); bounds.Dx() != int(reference.Width) || bounds.Dy() != int(reference.Height) {
		return nil, errors.New("sidecar dimensions do not match reference")
	}
	return result, result.Validate()
}

func writeImage(dir string, source *protocol.Image) error {
	if source == nil {
		return nil
	}
	reference := imageReference(source)
	path := filepath.Join(dir, reference.SHA256+".png")
	if _, err := os.Lstat(path); err == nil {
		existing, err := loadImage(dir, reference)
		if err != nil {
			return fmt.Errorf("history: existing image sidecar: %w", err)
		}
		if !bytes.Equal(existing.Data, source.Data) {
			return errors.New("history: image sidecar content mismatch")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("history: inspect image sidecar: %w", err)
	}
	temporary, err := os.CreateTemp(dir, ".image.tmp-")
	if err != nil {
		return fmt.Errorf("history: create image temporary file: %w", err)
	}
	temporaryName := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryName)
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("history: secure image temporary file: %w", err)
	}
	if _, err := temporary.Write(source.Data); err != nil {
		return fmt.Errorf("history: write image temporary file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("history: sync image temporary file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("history: close image temporary file: %w", err)
	}
	if err := os.Link(temporaryName, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			return writeImage(dir, source)
		}
		return fmt.Errorf("history: commit image: %w", err)
	}
	return syncDirectory(dir)
}

func cleanupImages(dir string, entries []protocol.HistoryEntry) error {
	references := make(map[string]struct{})
	for _, entry := range entries {
		if reference := imageReference(entry.Image); reference != nil {
			references[reference.SHA256+".png"] = struct{}{}
		}
	}
	directory, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("history: list image directory: %w", err)
	}
	changed := false
	for _, item := range directory {
		if filepath.Ext(item.Name()) != ".png" {
			continue
		}
		if _, referenced := references[item.Name()]; referenced {
			continue
		}
		if err := os.Remove(filepath.Join(dir, item.Name())); err != nil {
			return fmt.Errorf("history: remove orphan image: %w", err)
		}
		changed = true
	}
	if changed {
		return syncDirectory(dir)
	}
	return nil
}

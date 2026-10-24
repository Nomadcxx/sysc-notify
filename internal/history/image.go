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

// readSidecar reads a sidecar file. It is a variable so a test can count how
// many sidecars a commit reads.
var readSidecar = readPrivateRegularFile

// imageMeta is what the store remembers about an image once it has been
// hashed: the digest that names its sidecar and the dimensions the sidecar
// records. The digest is computed once, when the image is prepared or loaded,
// and reused for every later write and for every encoded document.
type imageMeta struct {
	digest string
	width  uint32
	height uint32
}

// PreparedImage is a validated image normalized for retained history.
// Its contents stay private so Store.AddPrepared can skip repeating the PNG work.
type PreparedImage struct {
	image *protocol.Image
	meta  *imageMeta
}

// PrepareImage validates and downsizes an image for retained history.
func PrepareImage(source *protocol.Image) (PreparedImage, error) {
	image, err := normalizeHistoryImage(source)
	if err != nil {
		return PreparedImage{}, err
	}
	if image == nil {
		return PreparedImage{}, nil
	}
	return PreparedImage{image: image, meta: newImageMeta(image)}, nil
}

// newImageMeta hashes an image once and keeps the digest that names its
// sidecar, so no later write or document has to hash it again.
func newImageMeta(source *protocol.Image) *imageMeta {
	digest := sha256.Sum256(source.Data)
	return &imageMeta{digest: hex.EncodeToString(digest[:]), width: source.Width, height: source.Height}
}

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

// imageReference builds the on-disk reference of an image from the digest
// that was computed when the image was prepared or loaded.
func imageReference(meta *imageMeta) *diskImage {
	if meta == nil {
		return nil
	}
	return &diskImage{SHA256: meta.digest, Width: meta.width, Height: meta.height}
}

// loadImage reads a sidecar and verifies it against the reference that named
// it. It returns the image and the metadata that names its sidecar, so the
// digest is not computed again later.
func loadImage(dir string, reference *diskImage) (*protocol.Image, *imageMeta, error) {
	if reference == nil {
		return nil, nil, nil
	}
	if len(reference.SHA256) != sha256.Size*2 || reference.Width == 0 || reference.Height == 0 ||
		reference.Width > historyImageLongEdge || reference.Height > historyImageLongEdge {
		return nil, nil, errors.New("invalid image reference")
	}
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil || hex.EncodeToString(digest) != reference.SHA256 {
		return nil, nil, errors.New("invalid image digest")
	}
	path := filepath.Join(dir, reference.SHA256+".png")
	contents, err := readSidecar(path, protocol.MaxWireImageBytes)
	if err != nil {
		return nil, nil, err
	}
	actual := sha256.Sum256(contents)
	if !bytes.Equal(actual[:], digest) {
		return nil, nil, errors.New("image digest mismatch")
	}
	result := &protocol.Image{MediaType: "image/png", Width: reference.Width, Height: reference.Height, Data: contents}
	decoded, err := png.Decode(bytes.NewReader(contents))
	if err != nil {
		return nil, nil, fmt.Errorf("decode sidecar: %w", err)
	}
	if bounds := decoded.Bounds(); bounds.Dx() != int(reference.Width) || bounds.Dy() != int(reference.Height) {
		return nil, nil, errors.New("sidecar dimensions do not match reference")
	}
	if err := result.Validate(); err != nil {
		return nil, nil, err
	}
	meta := &imageMeta{digest: reference.SHA256, width: reference.Width, height: reference.Height}
	return result, meta, nil
}

// writeImage stores the image as a content-addressed sidecar. A sidecar that
// is already on disk is left alone: this process wrote it, or the startup path
// verified it, and reading it back to hash and decode it again is the work
// this path avoids. A sidecar that was removed while the daemon runs is
// written again, because the stat says it is gone.
func writeImage(dir string, source *protocol.Image, meta *imageMeta) error {
	if source == nil || meta == nil {
		return nil
	}
	path := filepath.Join(dir, meta.digest+".png")
	if _, err := os.Lstat(path); err == nil {
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
			return writeImage(dir, source, meta)
		}
		return fmt.Errorf("history: commit image: %w", err)
	}
	return syncDirectory(dir)
}

// cleanupImages removes the sidecars that no entry refers to. The references
// come from the digests the store already knows, so no image is hashed again.
func cleanupImages(dir string, entries []retained) error {
	references := make(map[string]struct{}, len(entries))
	for i := range entries {
		if entries[i].meta != nil {
			references[entries[i].meta.digest+".png"] = struct{}{}
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

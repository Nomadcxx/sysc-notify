package history

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"image"
	"image/png"
	"io"
	"os"
	"path/filepath"

	"github.com/Nomadcxx/sysc-notify/protocol"
)

const historyImageLongEdge = 96

// hashSidecar is a variable so a test can count digest verifications.
var hashSidecar = hashPrivateRegularFile

// imageMeta is what the store remembers about an image once it has been
// hashed: the digest and dimensions, plus the verified file identity used to
// skip reads while the sidecar remains unchanged. A changed file is hashed
// again before the store trusts it.
type imageMeta struct {
	digest   string
	width    uint32
	height   uint32
	fileInfo os.FileInfo
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
	contents, err := readPrivateRegularFile(path, protocol.MaxWireImageBytes)
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
	info, err := privateRegularFileInfo(path)
	if err != nil {
		return nil, nil, err
	}
	meta := &imageMeta{digest: reference.SHA256, width: reference.Width, height: reference.Height, fileInfo: info}
	return result, meta, nil
}

// writeImage stores the image as a content-addressed sidecar. An existing
// sidecar is trusted only while its private file identity and metadata match
// the verified snapshot. A changed sidecar is hashed and restored from the
// retained image when its digest no longer matches.
func writeImage(dir string, source *protocol.Image, meta *imageMeta, buffer []byte) error {
	if source == nil || meta == nil {
		return nil
	}
	path := filepath.Join(dir, meta.digest+".png")
	replace := false
	if info, err := privateRegularFileInfo(path); err == nil {
		if sameSidecarInfo(meta.fileInfo, info) {
			return nil
		}
		digest, verifiedInfo, err := hashSidecar(path, protocol.MaxWireImageBytes, buffer)
		if err == nil {
			if hex.EncodeToString(digest[:]) == meta.digest {
				meta.fileInfo = verifiedInfo
				return nil
			}
			replace = true
		} else if errors.Is(err, errFileTooLarge) {
			replace = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("history: inspect image sidecar: %w", err)
		}
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
	if replace {
		if err := os.Rename(temporaryName, path); err != nil {
			return fmt.Errorf("history: restore image sidecar: %w", err)
		}
		if err := syncDirectory(dir); err != nil {
			return err
		}
		info, err := privateRegularFileInfo(path)
		if err != nil {
			return err
		}
		meta.fileInfo = info
		return nil
	}
	if err := os.Link(temporaryName, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			return writeImage(dir, source, meta, buffer)
		}
		return fmt.Errorf("history: commit image: %w", err)
	}
	if err := os.Remove(temporaryName); err != nil {
		return fmt.Errorf("history: remove image temporary file: %w", err)
	}
	if err := syncDirectory(dir); err != nil {
		return err
	}
	info, err := privateRegularFileInfo(path)
	if err != nil {
		return err
	}
	meta.fileInfo = info
	return nil
}

// hashPrivateRegularFile hashes a sidecar without allocating its full
// contents. The file identity is checked before and after reading so a path
// replaced during verification cannot be trusted.
func hashPrivateRegularFile(path string, limit int64, buffer []byte) ([sha256.Size]byte, os.FileInfo, error) {
	var digest [sha256.Size]byte
	info, err := privateRegularFileInfo(path)
	if err != nil {
		return digest, nil, err
	}
	if info.Size() > limit {
		return digest, nil, fmt.Errorf("%w: %q exceeds %d bytes", errFileTooLarge, path, limit)
	}
	file, err := os.Open(path)
	if err != nil {
		return digest, nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return digest, nil, err
	}
	if !opened.Mode().IsRegular() || opened.Mode().Perm() != 0o600 || !os.SameFile(info, opened) {
		return digest, nil, fmt.Errorf("history: unsafe file %q", path)
	}
	if len(buffer) == 0 {
		buffer = make([]byte, 32*1024)
	}
	h := sha256.New()
	size, err := io.CopyBuffer(h, io.LimitReader(file, limit+1), buffer)
	if err != nil {
		return digest, nil, err
	}
	if size > limit {
		return digest, nil, fmt.Errorf("%w: %q exceeds %d bytes", errFileTooLarge, path, limit)
	}
	current, err := file.Stat()
	if err != nil {
		return digest, nil, err
	}
	pathInfo, err := privateRegularFileInfo(path)
	if err != nil {
		return digest, nil, err
	}
	if current.Size() != opened.Size() || !current.ModTime().Equal(opened.ModTime()) ||
		!sameFileChangeTime(opened, current) || !os.SameFile(current, pathInfo) ||
		pathInfo.Size() != current.Size() || !pathInfo.ModTime().Equal(current.ModTime()) ||
		!sameFileChangeTime(current, pathInfo) {
		return digest, nil, fmt.Errorf("history: image sidecar changed during verification: %q", path)
	}
	copy(digest[:], h.Sum(digest[:0]))
	return digest, pathInfo, nil
}

func privateRegularFileInfo(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return nil, fmt.Errorf("history: unsafe file %q", path)
	}
	return info, nil
}

// sameSidecarInfo also compares Linux ctime, so an in-place edit with its
// modtime restored still invalidates the snapshot.
func sameSidecarInfo(verified, current os.FileInfo) bool {
	return verified != nil && os.SameFile(verified, current) &&
		verified.Size() == current.Size() && verified.ModTime().Equal(current.ModTime()) &&
		sameFileChangeTime(verified, current)
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

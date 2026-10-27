package presenter

import (
	"testing"

	"github.com/Nomadcxx/sysc-notify/protocol"
)

func testWireImage(size int) *protocol.Image {
	return &protocol.Image{MediaType: "image/png", Width: 8, Height: 8, Data: make([]byte, size)}
}

func snapshotWithImages(active, history, size int) protocol.Snapshot {
	snapshot := protocol.Snapshot{Sequence: 7}
	for i := range active {
		snapshot.Active = append(snapshot.Active, protocol.Notification{
			ID: uint32(i + 1), Summary: "active", Image: testWireImage(size),
		})
	}
	for i := range history {
		snapshot.History = append(snapshot.History, protocol.HistoryEntry{
			ID: uint32(active + i + 1), Summary: "history", Image: testWireImage(size),
		})
	}
	return snapshot
}

// The frame has to hold every record, so the images are what gives way: the
// oldest ones are dropped and the records still arrive. The snapshot the
// caller owns keeps its images.
func TestMarshalSnapshotFramesWithinTheLimit(t *testing.T) {
	cases := []struct {
		name        string
		limit       int
		snapshot    protocol.Snapshot
		wantRecords int
		wantImages  []uint32
		wantError   bool
	}{
		{
			name:  "no images",
			limit: 4096,
			snapshot: protocol.Snapshot{Sequence: 3, Active: []protocol.Notification{
				{ID: 1, Summary: "one"}, {ID: 2, Summary: "two"},
			}},
			wantRecords: 2,
		},
		{
			name:        "images fit",
			limit:       4096,
			snapshot:    snapshotWithImages(2, 0, 512),
			wantRecords: 2, wantImages: []uint32{1, 2},
		},
		{
			// Three images do not fit the frame, so the oldest one is
			// dropped and the two newest ones stay.
			name:        "oldest image dropped",
			limit:       4096,
			snapshot:    snapshotWithImages(3, 0, 1024),
			wantRecords: 3, wantImages: []uint32{2, 3},
		},
		{
			// The images are dropped oldest first and the active records come
			// first, so the history image survives the active one.
			name:        "history image kept over active",
			limit:       2048,
			snapshot:    snapshotWithImages(1, 1, 1024),
			wantRecords: 2, wantImages: []uint32{2},
		},
		{
			name:  "bodies exceed the frame",
			limit: 4096,
			snapshot: protocol.Snapshot{Sequence: 9, Active: []protocol.Notification{
				{ID: 1, Summary: "huge", Body: string(make([]byte, 8192))},
			}},
			wantError: true,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			// The records that carry an image are recorded before the call:
			// the snapshot the caller owns must keep them.
			withImages := imageRecordIDs(test.snapshot)
			frame, err := marshalSnapshotWithin(test.snapshot, test.limit)
			if test.wantError {
				if err == nil {
					t.Fatal("marshalSnapshot accepted a snapshot that does not fit a frame")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(frame) > test.limit {
				t.Fatalf("frame is %d bytes, over the %d byte limit", len(frame), test.limit)
			}
			decoded := decodeSnapshotFrame(t, frame)
			records := len(decoded.Active) + len(decoded.History)
			if records != test.wantRecords {
				t.Fatalf("frame holds %d records, want %d", records, test.wantRecords)
			}
			for _, id := range test.wantImages {
				if !hasImage(decoded, id) {
					t.Fatalf("frame dropped the image of record %d", id)
				}
			}
			for _, id := range recordIDs(decoded) {
				if !containsID(test.wantImages, id) && hasImage(decoded, id) {
					t.Fatalf("frame kept the image of record %d, which does not fit", id)
				}
			}
			// The snapshot the caller owns must not lose its images: the
			// marshalling drops images from its own copy.
			for _, id := range withImages {
				if !hasImage(test.snapshot, id) {
					t.Fatalf("marshalSnapshot cleared the image of record %d", id)
				}
			}
		})
	}
}

// The frame the presenter sends is bounded by the protocol limit, not by the
// limit a test picks.
func TestMarshalSnapshotUsesTheProtocolFrameLimit(t *testing.T) {
	frame, err := marshalSnapshot(snapshotWithImages(2, 0, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if len(frame) > int(protocol.MaxFrameSize) {
		t.Fatalf("frame is %d bytes, over the %d byte limit", len(frame), protocol.MaxFrameSize)
	}
	decoded := decodeSnapshotFrame(t, frame)
	for _, id := range []uint32{1, 2} {
		if !hasImage(decoded, id) {
			t.Fatalf("frame dropped the image of record %d", id)
		}
	}
}

// The cost model decides which images fit, so it has to be the size of the
// image as the frame carries it. Measured against the frame, the model
// cannot be a byte short: a short estimate keeps one image too many, the
// frame goes over the limit, and a handshake fails where a snapshot without
// one image would have done.
func TestWireImageCostMatchesTheFrameItIsAddedTo(t *testing.T) {
	image := &protocol.Image{MediaType: "image/png", Width: 100, Height: 100, Data: []byte("ABCD")}
	withImage := protocol.Snapshot{Sequence: 1, Active: []protocol.Notification{
		{ID: 1, Summary: "one", Image: image},
	}}
	withoutImage := protocol.Snapshot{Sequence: 1, Active: []protocol.Notification{
		{ID: 1, Summary: "one"},
	}}
	with, err := marshalEnvelope(protocol.KindSnapshot, 0, withImage.Sequence, withImage)
	if err != nil {
		t.Fatal(err)
	}
	without, err := marshalEnvelope(protocol.KindSnapshot, 0, withoutImage.Sequence, withoutImage)
	if err != nil {
		t.Fatal(err)
	}
	delta := len(with) - len(without)
	if got := wireImageCost(image); got != delta {
		t.Fatalf("wireImageCost = %d, the image adds %d bytes to the frame", got, delta)
	}
}

// The limit the framing is given is the size of the bodies plus what the
// images cost, so every image fits exactly. The frame that comes out has to
// be within the limit: the cost model is what the framing decides with, so a
// frame bigger than the model allows cannot be written.
func TestMarshalSnapshotHonoursALimitBuiltFromTheCostModel(t *testing.T) {
	const images = 12
	snapshot := snapshotWithImages(0, images, 16)
	bodies := snapshot
	bodies.History = append([]protocol.HistoryEntry(nil), snapshot.History...)
	for i := range bodies.History {
		bodies.History[i].Image = nil
	}
	base, err := marshalEnvelope(protocol.KindSnapshot, 0, snapshot.Sequence, bodies)
	if err != nil {
		t.Fatal(err)
	}
	limit := len(base) + images*wireImageCost(snapshot.History[0].Image)
	frame, err := marshalSnapshotWithin(snapshot, limit)
	if err != nil {
		t.Fatalf("marshalSnapshotWithin refused a snapshot that fits the limit: %v", err)
	}
	if len(frame) > limit {
		t.Fatalf("frame is %d bytes, over the %d byte limit", len(frame), limit)
	}
}

// A cost model that under-prices an image keeps one image too many. The
// frame has to come out inside the limit anyway: the snapshot is worth less
// than the handshake that carries it.
func TestMarshalSnapshotDropsAnImageWhenTheCostModelIsShort(t *testing.T) {
	const images = 4
	snapshot := snapshotWithImages(0, images, 16)
	bodies := snapshot
	bodies.History = append([]protocol.HistoryEntry(nil), snapshot.History...)
	for i := range bodies.History {
		bodies.History[i].Image = nil
	}
	base, err := marshalEnvelope(protocol.KindSnapshot, 0, snapshot.Sequence, bodies)
	if err != nil {
		t.Fatal(err)
	}
	// The limit leaves room for the images as they are, and the model
	// prices every one of them a byte short, so it keeps one too many.
	limit := len(base) + images*(wireImageCost(snapshot.History[0].Image)-1)
	short := func(image *protocol.Image) int { return wireImageCost(image) - 1 }
	frame, err := marshalSnapshotWithinCost(snapshot, limit, short)
	if err != nil {
		t.Fatalf("marshalSnapshotWithinCost refused a snapshot that fits the limit: %v", err)
	}
	if len(frame) > limit {
		t.Fatalf("frame is %d bytes, over the %d byte limit", len(frame), limit)
	}
	decoded := decodeSnapshotFrame(t, frame)
	kept := 0
	for _, id := range recordIDs(decoded) {
		if hasImage(decoded, id) {
			kept++
		}
	}
	if kept != images-1 {
		t.Fatalf("the frame holds %d images, want %d: the backstop did not give one up", kept, images-1)
	}
}

func hasImage(snapshot protocol.Snapshot, id uint32) bool {
	for _, notification := range snapshot.Active {
		if notification.ID == id {
			return notification.Image != nil
		}
	}
	for _, entry := range snapshot.History {
		if entry.ID == id {
			return entry.Image != nil
		}
	}
	return false
}

func recordIDs(snapshot protocol.Snapshot) []uint32 {
	ids := make([]uint32, 0, len(snapshot.Active)+len(snapshot.History))
	for _, notification := range snapshot.Active {
		ids = append(ids, notification.ID)
	}
	for _, entry := range snapshot.History {
		ids = append(ids, entry.ID)
	}
	return ids
}

func imageRecordIDs(snapshot protocol.Snapshot) []uint32 {
	ids := make([]uint32, 0, len(snapshot.Active)+len(snapshot.History))
	for _, notification := range snapshot.Active {
		if notification.Image != nil {
			ids = append(ids, notification.ID)
		}
	}
	for _, entry := range snapshot.History {
		if entry.Image != nil {
			ids = append(ids, entry.ID)
		}
	}
	return ids
}

func containsID(ids []uint32, id uint32) bool {
	for _, candidate := range ids {
		if candidate == id {
			return true
		}
	}
	return false
}

func decodeSnapshotFrame(t *testing.T, frame []byte) protocol.Snapshot {
	t.Helper()
	var envelope protocol.Envelope
	if err := protocol.DecodeStrict(frame, &envelope); err != nil {
		t.Fatal(err)
	}
	var snapshot protocol.Snapshot
	if err := protocol.DecodeStrict(envelope.Payload, &snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

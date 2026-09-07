package protocol

import "testing"

func TestValidateHistoryRemoveRequiresIDs(t *testing.T) {
	if err := (Command{Kind: CommandHistoryRemove}).Validate(); err == nil {
		t.Fatal("empty id list accepted")
	}
	if err := (Command{Kind: CommandHistoryRemove, IDs: []uint32{7}}).Validate(); err != nil {
		t.Fatalf("valid remove rejected: %v", err)
	}
}

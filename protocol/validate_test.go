package protocol

import (
	"strings"
	"testing"
	"time"
)

func TestNotificationRejectsOversizedReplyPlaceholder(t *testing.T) {
	notification := Notification{
		ID: 1, Summary: "chat", Timestamp: time.Unix(0, 0).UTC(), Urgency: UrgencyNormal,
		ReplyPlaceholder: strings.Repeat("a", MaxBodyBytes+1),
	}
	if err := notification.Validate(); err == nil {
		t.Fatal("oversized reply placeholder accepted")
	}
	notification.ReplyPlaceholder = "Reply to Alice"
	if err := notification.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateHistoryRemoveRequiresIDs(t *testing.T) {
	if err := (Command{Kind: CommandHistoryRemove}).Validate(); err == nil {
		t.Fatal("empty id list accepted")
	}
	if err := (Command{Kind: CommandHistoryRemove, IDs: []uint32{7}}).Validate(); err != nil {
		t.Fatalf("valid remove rejected: %v", err)
	}
}

func TestValidateProducerBounds(t *testing.T) {
	value := int32(15)
	valid := ProducerRequest{
		Key: "sysc-shell:battery-low", AppName: "sysc-shell", Summary: "Battery low",
		Body: "Battery is at 15%.", Urgency: UrgencyCritical, Value: &value,
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}

	mutations := map[string]func(*ProducerRequest){
		"empty key":        func(r *ProducerRequest) { r.Key = "" },
		"long key":         func(r *ProducerRequest) { r.Key = string(make([]byte, MaxProducerKeyBytes+1)) },
		"invalid key utf8": func(r *ProducerRequest) { r.Key = string([]byte{0xff}) },
		"long body":        func(r *ProducerRequest) { r.Body = string(make([]byte, MaxBodyBytes+1)) },
		"urgency":          func(r *ProducerRequest) { r.Urgency = Urgency(9) },
		"value below zero": func(r *ProducerRequest) { v := int32(-1); r.Value = &v },
		"value above 100":  func(r *ProducerRequest) { v := int32(101); r.Value = &v },
		"expiry below -1":  func(r *ProducerRequest) { r.ExpireTimeoutMS = -2 },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			got := valid
			mutate(&got)
			if err := got.Validate(); err == nil {
				t.Fatal("Validate() accepted invalid producer request")
			}
		})
	}
	if err := (Command{Kind: CommandProducerPublish}).Validate(); err == nil {
		t.Fatal("publish without producer payload accepted")
	}
}

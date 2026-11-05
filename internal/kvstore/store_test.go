package kvstore

import "testing"

func TestApplyPutAndGet(t *testing.T) {
	s := NewStore()
	s.Apply(Command{Op: OpPut, Key: "a", Value: "1"})
	v, ok := s.Get("a")
	if !ok || v != "1" {
		t.Fatalf("expected a=1, got %q ok=%v", v, ok)
	}
}

func TestApplyDelete(t *testing.T) {
	s := NewStore()
	s.Apply(Command{Op: OpPut, Key: "a", Value: "1"})
	s.Apply(Command{Op: OpDelete, Key: "a"})
	_, ok := s.Get("a")
	if ok {
		t.Fatalf("expected a to be deleted")
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	s := NewStore()
	s.Apply(Command{Op: OpPut, Key: "a", Value: "1"})
	s.Apply(Command{Op: OpPut, Key: "b", Value: "2"})

	snap, err := s.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	restored := NewStore()
	if err := restored.RestoreSnapshot(snap); err != nil {
		t.Fatalf("restore: %v", err)
	}
	v, ok := restored.Get("b")
	if !ok || v != "2" {
		t.Fatalf("expected restored b=2, got %q ok=%v", v, ok)
	}
}

func TestCommandEncodeDecode(t *testing.T) {
	cmd := Command{Op: OpPut, Key: "x", Value: "y"}
	data, err := cmd.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := DecodeCommand(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded != cmd {
		t.Fatalf("round trip mismatch: got %+v want %+v", decoded, cmd)
	}
}

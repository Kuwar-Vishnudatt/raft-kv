package merkle

import (
	"bytes"
	"testing"
)

func TestIdenticalDataProducesIdenticalRoot(t *testing.T) {
	a := Build(map[string]string{"x": "1", "y": "2"})
	b := Build(map[string]string{"y": "2", "x": "1"}) // different insertion order
	if !bytes.Equal(a.RootHash(), b.RootHash()) {
		t.Fatal("identical key-value data should produce identical root hashes regardless of map order")
	}
}

func TestDifferentDataProducesDifferentRoot(t *testing.T) {
	a := Build(map[string]string{"x": "1"})
	b := Build(map[string]string{"x": "2"})
	if bytes.Equal(a.RootHash(), b.RootHash()) {
		t.Fatal("different values should produce different root hashes")
	}
}

func TestEmptyTreeHasAStableRoot(t *testing.T) {
	a := Build(map[string]string{})
	b := Build(map[string]string{})
	if !bytes.Equal(a.RootHash(), b.RootHash()) {
		t.Fatal("two empty trees should have the same root")
	}
}

func TestDivergentKeysFindsModifiedValue(t *testing.T) {
	a := map[string]string{"x": "1", "y": "2"}
	b := map[string]string{"x": "1", "y": "999"}
	diffs := DivergentKeys(a, b)
	if len(diffs) != 1 || diffs[0] != "y" {
		t.Fatalf("expected [y], got %v", diffs)
	}
}

func TestDivergentKeysFindsMissingAndExtraKeys(t *testing.T) {
	a := map[string]string{"x": "1"}
	b := map[string]string{"x": "1", "y": "2"}
	diffs := DivergentKeys(a, b)
	if len(diffs) != 1 || diffs[0] != "y" {
		t.Fatalf("expected [y], got %v", diffs)
	}
}

func TestDivergentKeysEmptyWhenEqual(t *testing.T) {
	a := map[string]string{"x": "1", "y": "2"}
	b := map[string]string{"x": "1", "y": "2"}
	if diffs := DivergentKeys(a, b); len(diffs) != 0 {
		t.Fatalf("expected no diffs, got %v", diffs)
	}
}

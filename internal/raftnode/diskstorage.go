package raftnode

import (
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/protobuf/proto"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// DiskStorage satisfies raft.Storage by delegating every read to an
// in-memory raft.MemoryStorage (fast, and MemoryStorage already implements
// all the index-math edge cases correctly), while mirroring every write to
// disk first. On startup, a fresh MemoryStorage is rebuilt by loading the
// last snapshot and replaying the WAL recorded since it -- this is what
// lets a node that's truly killed and restarted (not just network-
// partitioned) recover its committed state instead of rejoining as if it
// had always been empty.
type DiskStorage struct {
	dir string
	mem *raft.MemoryStorage

	mu      sync.Mutex
	walFile *os.File
}

const (
	walFileName   = "wal.log"
	snapFileName  = "snapshot"
	stateFileName = "hardstate"
)

// OpenDiskStorage loads any existing state from dir and returns whether
// state already existed (a restart) or this is a brand-new node.
func OpenDiskStorage(dir string) (*DiskStorage, bool, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, false, err
	}

	mem := raft.NewMemoryStorage()
	existed := false

	if data, err := os.ReadFile(filepath.Join(dir, snapFileName)); err == nil {
		existed = true
		var snap raftpb.Snapshot
		if err := proto.Unmarshal(data, &snap); err != nil {
			return nil, false, err
		}
		if err := mem.ApplySnapshot(&snap); err != nil {
			return nil, false, err
		}
	}

	if data, err := os.ReadFile(filepath.Join(dir, stateFileName)); err == nil {
		existed = true
		var hs raftpb.HardState
		if err := proto.Unmarshal(data, &hs); err != nil {
			return nil, false, err
		}
		if err := mem.SetHardState(&hs); err != nil {
			return nil, false, err
		}
	}

	records, err := readAllRecords(filepath.Join(dir, walFileName))
	if err != nil {
		return nil, false, err
	}
	if len(records) > 0 {
		existed = true
		entries := make([]*raftpb.Entry, 0, len(records))
		for _, r := range records {
			var e raftpb.Entry
			if err := proto.Unmarshal(r, &e); err != nil {
				return nil, false, err
			}
			entries = append(entries, &e)
		}
		if err := mem.Append(entries); err != nil {
			return nil, false, err
		}
	}

	walFile, err := os.OpenFile(filepath.Join(dir, walFileName), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, false, err
	}

	return &DiskStorage{dir: dir, mem: mem, walFile: walFile}, existed, nil
}

func (d *DiskStorage) Append(entries []*raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, e := range entries {
		data, err := proto.Marshal(e)
		if err != nil {
			return err
		}
		if err := writeRecord(d.walFile, data); err != nil {
			return err
		}
	}
	if err := d.walFile.Sync(); err != nil {
		return err
	}
	return d.mem.Append(entries)
}

func (d *DiskStorage) SetHardState(st *raftpb.HardState) error {
	data, err := proto.Marshal(st)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(d.dir, stateFileName), data, 0o644); err != nil {
		return err
	}
	return d.mem.SetHardState(st)
}

// CreateSnapshot persists the state machine snapshot to disk and starts a
// fresh WAL file -- everything before the snapshot's index is now covered by
// the snapshot itself, so the old WAL segment can be dropped entirely rather
// than compacted in place.
func (d *DiskStorage) CreateSnapshot(i uint64, cs *raftpb.ConfState, data []byte) (*raftpb.Snapshot, error) {
	snap, err := d.mem.CreateSnapshot(i, cs, data)
	if err != nil {
		return snap, err
	}
	snapData, err := proto.Marshal(snap)
	if err != nil {
		return snap, err
	}
	if err := os.WriteFile(filepath.Join(d.dir, snapFileName), snapData, 0o644); err != nil {
		return snap, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.walFile.Close(); err != nil {
		return snap, err
	}
	if err := os.Truncate(filepath.Join(d.dir, walFileName), 0); err != nil {
		return snap, err
	}
	walFile, err := os.OpenFile(filepath.Join(d.dir, walFileName), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return snap, err
	}
	d.walFile = walFile
	return snap, nil
}

func (d *DiskStorage) Compact(compactIndex uint64) error {
	return d.mem.Compact(compactIndex)
}

func (d *DiskStorage) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.walFile.Close()
}

// --- raft.Storage interface: reads are all served from the in-memory copy ---

func (d *DiskStorage) InitialState() (*raftpb.HardState, *raftpb.ConfState, error) {
	return d.mem.InitialState()
}

func (d *DiskStorage) Entries(lo, hi, maxSize uint64) ([]*raftpb.Entry, error) {
	return d.mem.Entries(lo, hi, maxSize)
}

func (d *DiskStorage) Term(i uint64) (uint64, error)       { return d.mem.Term(i) }
func (d *DiskStorage) LastIndex() (uint64, error)          { return d.mem.LastIndex() }
func (d *DiskStorage) FirstIndex() (uint64, error)         { return d.mem.FirstIndex() }
func (d *DiskStorage) Snapshot() (*raftpb.Snapshot, error) { return d.mem.Snapshot() }

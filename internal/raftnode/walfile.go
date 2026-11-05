package raftnode

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
)

// Each WAL record is framed as [4-byte length][4-byte CRC32 of payload][payload].
// The checksum exists for exactly the reason real WALs carry one: a process
// killed mid-write can leave a truncated or torn final record on disk, and
// replay must detect that and stop there rather than feed garbage into Raft
// -- losing the unwritten last record is correct and expected (it was never
// acknowledged), silently replaying corrupt bytes as if they were valid is
// not.
func writeRecord(w io.Writer, payload []byte) error {
	var header [8]byte
	binary.BigEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// readAllRecords reads every complete, checksum-valid record from the start
// of the file. It stops (without error) at EOF, at a short/partial trailing
// record, or at the first checksum mismatch -- all three indicate "this is
// as far as a torn write got," not a reason to fail startup.
func readAllRecords(path string) ([][]byte, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var records [][]byte
	var header [8]byte
	for {
		if _, err := io.ReadFull(f, header[:]); err != nil {
			break // EOF or short read -- stop here, see doc comment above
		}
		length := binary.BigEndian.Uint32(header[0:4])
		wantCRC := binary.BigEndian.Uint32(header[4:8])

		payload := make([]byte, length)
		if _, err := io.ReadFull(f, payload); err != nil {
			break // truncated record from a torn write
		}
		if crc32.ChecksumIEEE(payload) != wantCRC {
			break // corrupt record -- treat everything after as not durably written
		}
		records = append(records, payload)
	}
	return records, nil
}

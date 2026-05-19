//go:build linux

package ebpf

import "io"

// byteReaderAt wraps a byte slice and implements io.ReaderAt (needed by
// ebpf.LoadCollectionSpecFromReader which accepts an io.ReaderAt).
type byteReaderAt struct {
	b []byte
}

func newByteReaderAt(b []byte) io.ReaderAt {
	return &byteReaderAt{b: b}
}

func (r *byteReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(r.b)) {
		return 0, io.EOF
	}
	n := copy(p, r.b[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

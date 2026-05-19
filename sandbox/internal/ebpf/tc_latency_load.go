//go:build linux

// Loader for the compiled eBPF bytecode. The .o file is produced by
// `make ebpf-build` (runs clang inside a Linux Docker container) and committed
// to the repo so CI and prod builds don't need a local LLVM toolchain.
package ebpf

import (
	_ "embed"

	"github.com/cilium/ebpf"
)

//go:embed bpf/tc_latency.o
var tcLatencyBytecode []byte

func loadTcLatencySpec() (*ebpf.CollectionSpec, error) {
	return ebpf.LoadCollectionSpecFromReader(
		// bytes.NewReader would also work; readerFromBytes satisfies io.ReaderAt
		newByteReaderAt(tcLatencyBytecode),
	)
}

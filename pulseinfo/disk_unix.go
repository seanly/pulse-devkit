//go:build unix

package pulseinfo

import (
	"syscall"

	"github.com/seanly/pulse-devkit/pulse"
)

func disks(mounts []string) map[string]pulse.DiskFact {
	out := make(map[string]pulse.DiskFact, len(mounts))
	for _, m := range mounts {
		var st syscall.Statfs_t
		if err := syscall.Statfs(m, &st); err != nil {
			continue
		}
		bsize := uint64(st.Bsize)
		out[m] = pulse.DiskFact{
			TotalBytes: uint64(st.Blocks) * bsize,
			FreeBytes:  uint64(st.Bavail) * bsize,
		}
	}
	return out
}

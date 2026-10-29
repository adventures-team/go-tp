package queuedclient

import (
	"hash/crc32"
	"strconv"
	"testing"
)

func TestCalcShard(t *testing.T) {
	// перло-совместимое значение: crc32("1") = 0x83DCEFB7, нечётное - шард 1 из 2
	if got := CalcShard(1, 2); got != 1 {
		t.Errorf("CalcShard(1, 2) = %d, want 1", got)
	}

	for _, id := range []uint64{0, 1, 2, 1122, 1<<32 + 7, ^uint64(0)} {
		for shardNum := uint(1); shardNum <= 5; shardNum++ {
			got := CalcShard(id, shardNum)
			want := uint(uint64(crc32.ChecksumIEEE([]byte(strconv.FormatUint(id, 10)))) % uint64(shardNum))

			if got != want {
				t.Errorf("CalcShard(%d, %d) = %d, want %d", id, shardNum, got, want)
			}

			if got >= shardNum {
				t.Errorf("CalcShard(%d, %d) = %d: out of range", id, shardNum, got)
			}
		}
	}
}

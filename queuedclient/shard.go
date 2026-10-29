package queuedclient

import (
	"hash/crc32"
	"strconv"
)

// CalcShard распределяет события по шардам в зависимости от хеша от ID события.
// Совместимо с перлом (алгоритм хеширования - crc от строкового десятичного
// представления eventID, распределение по шардам - остаток от деления).
//
// Так выбирают шард tp.BaseEvent.Shard и tp.Queue.Publish для событий с явно заданным ID -
// по этому же правилу событие с известным ID следует искать (и класть) в шардированном queued.
func CalcShard(eventID uint64, shardNum uint) uint {
	hash := uint64(crc32.ChecksumIEEE([]byte(strconv.FormatUint(eventID, 10))))

	return uint(hash % uint64(shardNum))
}

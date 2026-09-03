package queuedclient

import (
	"crypto/rand"
	"encoding/binary"
	"os"

	"gitlab.adv.team/go/tp.git/queueconfig"
)

// GetPID - хак для получения ID, обеспечивающего уникальность в протоколе при добавлении событий.
// Уникальность request_id обеспечивается в рамках client_ip + pid: queued/logic/queue.c:776
// Однако, могут быть следующие пробемы:
//   - NAT N:1 (уже один раз такое было)
//   - контейнеры/кубер, где pid может быть 1, или другой небольшой (при запуске в шелле/из скрипта, в странных кейсах - из-под systemd)
//
// Поэтому для небольших pid будем генерить 32-битный рандом ("честный" [crypt/random], честность зависит от ядра/рантайма контейнера)
//
// Чтобы генерить рандомный "pid" всегда, можно задать в онлайнконфе /queueds/main/min_pid больше максимально
// возможного pid для платформы (32768 для linux, 99999 для *bsd - поэтому лучше вписывать "совсем много", если планируется тестировать на макбуках без докера)
func GetPID(conf queueconfig.OnlineConf) (uint32, error) {
	pid := os.Getpid()
	minPID := conf.GetInt(ConfMinPID, DefaultMinPID)

	if pid >= minPID {
		return uint32(pid), nil
	}

	var buf [4]byte

	if _, err := rand.Read(buf[:]); err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint32(buf[:]), nil
}

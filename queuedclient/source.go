package queuedclient

import (
	"context"
	"strconv"
	"strings"
	"time"

	"gitlab.adv.team/go/tp.git/queueconfig"
)

// iprotoSource адаптирует легаси-схему конфига queued (/queueds/multi - CSV серверов,
// один сервер = один шард; параметры в /queueds/multi/*) к структурному интерфейсу
// config.Source библиотеки go-iprotoconn.
//
// Правила отображения:
//   - топология: "/shards/<n>/masters" -> n-й сервер из /queueds/multi (нумерация шардов
//     в go-iprotoconn - с 1); реплик у queued нет;
//   - параметры движка вычисляются из существующих перло-совместимых ключей
//     (соответствие описано у методов ниже);
//   - все неотображённые ключи возвращают переданный дефолт, т.е. действуют
//     дефолты go-iprotoconn.
//
// Особые случаи:
//   - "/retry-flags" всегда содержит "same": шард queued - это ровно один сервер,
//     а без RetrySame движок исключает уже опрошенные серверы и ретраям было бы
//     некуда идти;
//   - "/eager-dial" всегда true: соединения поднимаются сразу и поддерживаются,
//     как это делал easygo (go-iprotoconn >= v0.0.6);
//   - /queueds/multi/timeout исторически ограничивал одну попытку, поэтому он
//     отображается в "/attempt-timeout" (а также в connect/write), а полный дедлайн
//     "/request-timeout" вычисляется как retry_count * (timeout + retry_delay),
//     чтобы суммарное окно ретраев соответствовало старому поведению.
type iprotoSource struct {
	conf  *queueconfig.Config
	addrs []string        // адреса шардов из /queueds/multi (индекс = шард-1)
	ctx   context.Context // для логгирования ошибок парзинга значений конфига
}

func (s *iprotoSource) timeout() time.Duration {
	return s.conf.GetDuration(s.ctx, ConfTimeout, DefaultTimeout)
}

func (s *iprotoSource) retryDelay() time.Duration {
	return s.conf.GetDuration(s.ctx, ConfRetryDelay, DefaultRetryDelay)
}

func (s *iprotoSource) retryCount() int {
	if rc := s.conf.GetInt(ConfRetryCount, DefaultRetryCount); rc >= 1 {
		return rc
	}

	return 1
}

// shardMasters разбирает путь вида "/shards/<n>/masters" (без групп приоритетов
// и прочих суффиксов - они должны вернуть пустой результат).
func shardMasters(path string) (int, bool) {
	rest, ok := strings.CutPrefix(path, "/shards/")
	if !ok {
		return 0, false
	}

	numStr, ok := strings.CutSuffix(rest, "/masters")
	if !ok || strings.Contains(numStr, "/") {
		return 0, false
	}

	n, err := strconv.Atoi(numStr)
	if err != nil {
		return 0, false
	}

	return n, true
}

func (s *iprotoSource) GetStrings(path string, dflt []string) []string {
	if n, ok := shardMasters(path); ok {
		if n >= 1 && n <= len(s.addrs) {
			return []string{s.addrs[n-1]}
		}

		return nil
	}

	if path == "/retry-flags" {
		return []string{"safe", "same"}
	}

	return dflt
}

func (s *iprotoSource) GetDuration(path string, dflt time.Duration) time.Duration {
	switch path {
	case "/request-timeout":
		return time.Duration(s.retryCount()) * (s.timeout() + s.retryDelay())
	case "/attempt-timeout", "/connect-timeout", "/write-timeout":
		return s.timeout()
	case "/ping-interval":
		return s.conf.GetDuration(s.ctx, ConfPingInterval, DefaultPingInterval)
	case "/redial-interval", "/max-redial-interval":
		return s.conf.GetDuration(s.ctx, ConfRedialInterval, DefaultRedialInterval)
	case "/retry-backoff-base", "/retry-backoff-cap":
		return s.retryDelay()
	}

	return dflt
}

func (*iprotoSource) GetBool(path string, dflt bool) bool {
	switch path {
	case "/redial-forever", "/eager-dial":
		return true
	case "/cancels-enabled":
		// легаси-queued закрывает соединение, получив неизвестный ему управляющий
		// пакет Cancel (0xFF02) - проверено на реальном queued, поэтому отправка
		// Cancel выключена; тест TestCancelDoesNotKillConnection это перепроверяет.
		// Отмена запросов остаётся клиентской (tombstone-ы гасят опоздавшие ответы),
		// пакет в сеть не уходит.
		return false
	}

	return dflt
}

func (s *iprotoSource) GetInt(path string, dflt int) int {
	if path == "/max-tries" {
		return s.retryCount()
	}

	return dflt
}

func (*iprotoSource) GetFloat(path string, dflt float64) float64 {
	if path == "/retry-backoff-multiplier" {
		return 1.0 // фиксированная задержка между ретраями, как раньше
	}

	return dflt
}

func (*iprotoSource) GetString(_ string, dflt string) string { return dflt }

func (*iprotoSource) GetStringIfExists(_ string) (string, bool) { return "", false }

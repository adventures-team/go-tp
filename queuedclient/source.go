package queuedclient

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	advslog "github.com/adventures-team/go-adv-slog"
	"github.com/adventures-team/go-iprotoconn/config"

	"gitlab.adv.team/go/tp.git/queueconfig"
)

// iprotoSource адаптирует легаси-схему конфига queued (/queueds/multi - CSV серверов,
// один сервер = один шард; параметры в /queueds/multi/*) к структурному интерфейсу
// config.Source библиотеки go-iprotoconn.
//
// Правила отображения:
//   - топология: "/shards/<n>/masters" -> n-й сервер из /queueds/multi (нумерация шардов
//     в go-iprotoconn - с 1); реплик у queued нет; число шардов фиксируется при Dial,
//     его изменение требует перезапуска tp;
//   - параметры движка вычисляются из существующих перло-совместимых ключей
//     (соответствие описано у методов ниже);
//   - все неотображённые ключи возвращают переданный дефолт, т.е. действуют
//     дефолты go-iprotoconn.
//
// Горячее переподключение (config.Subscribable): подписки go-iprotoconn на виртуальные
// пути топологии (/shards, /masters, /replicas) отображаются в подписку на единственный
// реальный ключ /queueds/multi, а список адресов перечитывается из конфига при каждом
// пересчёте топологии - смена адресов серверов применяется без перезапуска. Число шардов
// при этом зафиксировано при Dial: от него зависит маршрутизация запросов в QueuedClient,
// поэтому список другой длины игнорируется с предупреждением в лог (см. servers).
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
	conf *queueconfig.Config
	ctx  context.Context // для логгирования: ошибки парзинга значений конфига, отвергнутые изменения списка серверов

	mu       sync.Mutex
	raw      string   // значение /queueds/multi, из которого получен addrs
	addrs    []string // адреса шардов (индекс = шард-1); длина зафиксирована при Dial
	rejected string   // последнее отвергнутое значение /queueds/multi - чтобы не повторять предупреждение
}

// go-iprotoconn узнаёт о поддержке подписок через type assertion: потеряй адаптер
// один из методов - горячее переподключение молча выключится (с предупреждением при
// старте). Компилятор этого не допустит.
var _ config.Subscribable = (*iprotoSource)(nil)

// newIprotoSource читает начальный список серверов; пустой список - ошибка [ErrNoServers].
func newIprotoSource(ctx context.Context, conf *queueconfig.Config) (*iprotoSource, error) {
	raw := conf.GetString(ConfServers, "")

	addrs := parseServers(raw)
	if len(addrs) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNoServers, ConfServers)
	}

	return &iprotoSource{conf: conf, ctx: ctx, raw: raw, addrs: addrs}, nil
}

// parseServers разбирает CSV-список серверов из /queueds/multi, пропуская пустые элементы.
func parseServers(raw string) []string {
	fields := queueconfig.CSVRegex.Split(raw, -1)
	addrs := make([]string, 0, len(fields))

	for _, server := range fields {
		if server != "" {
			addrs = append(addrs, server)
		}
	}

	return addrs
}

// servers возвращает актуальный список адресов шардов из /queueds/multi.
//
// Смена адресов при том же числе шардов применяется сразу (go-iprotoconn пересчитывает
// топологию по подписке, см. SubscribeChan). Список другой длины отвергается: число шардов
// зафиксировано при Dial (от него зависит маршрутизация запросов в QueuedClient), такое
// изменение требует перезапуска - о нём один раз пишется предупреждение, а действовать
// продолжает прежний список.
func (s *iprotoSource) servers() []string {
	raw := s.conf.GetString(ConfServers, "")

	s.mu.Lock()
	defer s.mu.Unlock()

	if raw == s.raw {
		return s.addrs
	}

	addrs := parseServers(raw)
	if len(addrs) != len(s.addrs) {
		if raw != s.rejected {
			s.rejected = raw

			advslog.Ctx(s.ctx).Warn("shard count in "+ConfServers+" changed, keeping the current server list until restart",
				"shards", len(s.addrs), "new_shards", len(addrs), "value", raw)
		}

		return s.addrs
	}

	s.raw, s.addrs = raw, addrs

	return addrs
}

// SubscribeChan - config.Subscribable: любая подписка на виртуальные пути топологии
// go-iprotoconn - это подписка на реальный ключ /queueds/multi, из которого топология
// и вычисляется. Семантика каналов - onlineconf-go/v2 (см. [queueconfig.OnlineConf]).
func (s *iprotoSource) SubscribeChan(_ string, ch chan<- struct{}) error {
	return s.conf.SubscribeChan(ConfServers, ch)
}

// UnsubscribeChan - см. SubscribeChan.
func (s *iprotoSource) UnsubscribeChan(_ string, ch chan<- struct{}) {
	s.conf.UnsubscribeChan(ConfServers, ch)
}

// SubscribeChanSubtree - см. SubscribeChan: параметры /queueds/multi/* на топологию
// не влияют, поэтому и для поддерева достаточно подписки на само значение ключа.
func (s *iprotoSource) SubscribeChanSubtree(_ string, ch chan<- struct{}) error {
	return s.conf.SubscribeChan(ConfServers, ch)
}

// UnsubscribeChanSubtree - см. SubscribeChan.
func (s *iprotoSource) UnsubscribeChanSubtree(_ string, ch chan<- struct{}) {
	s.conf.UnsubscribeChan(ConfServers, ch)
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
		if addrs := s.servers(); n >= 1 && n <= len(addrs) {
			return []string{addrs[n-1]}
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

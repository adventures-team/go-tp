//go:build integration

package tptest

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	advmetricsset "github.com/adventures-team/go-adv-metrics/set"
	advslog "github.com/adventures-team/go-adv-slog"
	"github.com/adventures-team/go-queuedproto"
	"github.com/onlineconf/onlineconf-go/v2"

	"gitlab.adv.team/go/tp.git/queueconfig"
	"gitlab.adv.team/go/tp.git/queuedclient"
)

// staticConf - минимальная статическая реализация queueconfig.OnlineConf для теста.
type staticConf map[string]string

func (c staticConf) GetString(path, dflt string) string {
	if v, ok := c[path]; ok {
		return v
	}

	return dflt
}

func (c staticConf) GetStringIfExists(path string) (string, bool) {
	v, ok := c[path]
	return v, ok
}

func (c staticConf) GetInt(path string, dflt int) int {
	if v, ok := c[path]; ok {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}

	return dflt
}

func (c staticConf) GetIntErr(path string) (int, error) {
	if v, ok := c[path]; ok {
		return strconv.Atoi(v)
	}

	return 0, onlineconf.ErrNotFound
}

func (staticConf) Subscribe(string) (chan struct{}, error) { return make(chan struct{}), nil }

func (staticConf) SubscribeChan(string, chan<- struct{}) error { return nil }

func (staticConf) UnsubscribeChan(string, chan<- struct{}) {}

func (c staticConf) Subtree(string) staticConf { return c }

// TestCancelDoesNotKillConnection: go-iprotoconn при отмене/таймауте запроса
// посылает управляющий пакет Cancel (0xFF02), которого легаси-queued не знает.
//
// Часть 1 (сырой сокет) документирует фактическое поведение queued: исторически
// он ЗАКРЫВАЕТ соединение, получив неизвестный тип сообщения (проверено 2026-08-21;
// поэтому в go-iprotoconn добавлен ключ /cancels-enabled, а адаптер queuedclient
// выключает отправку Cancel). Если когда-нибудь queued научится игнорировать
// неизвестные типы, тест сообщит об этом - и отправку можно будет включить обратно.
//
// Часть 2 проверяет собственно митигейшн: под форсированными таймаутами клиент
// не посылает Cancel (iproto_cancels_sent_total == 0) и соединение не закрывается
// ни одной из сторон (iproto_connections_closed_total == 0).
//
// Сервер адресуется как 127.0.0.2 (тот же докерный queued: порт слушается на 0.0.0.0):
// go-iprotoconn дедуплицирует соединения по строке адреса process-wide, а тесту нужно
// отдельное соединение с собственным реестром метрик, не разделяемое с другими тестами.
func TestCancelDoesNotKillConnection(t *testing.T) {
	ctx := advslog.NewContext(context.Background(), advslog.InitTest(t))

	// --- часть 1: фактическое поведение queued при получении Cancel ---
	if rawCancelProbe(t, "127.0.0.1:20000") {
		t.Log("queued now SURVIVES an unknown Cancel (0xFF02) control message: " +
			"consider re-enabling /cancels-enabled in queuedclient/source.go - it is off only because of this legacy behavior")
	} else {
		t.Log("queued closes the connection on Cancel (0xFF02) - known legacy behavior, " +
			"queuedclient keeps /cancels-enabled=false")
	}

	// --- часть 2: под таймаутами клиент не шлёт Cancel и соединение живёт ---
	conf := staticConf{
		queuedclient.ConfServers: "127.0.0.2:20000",
		queuedclient.ConfTimeout: "1", // секунда на попытку - чтобы ретраи теста не растягивались
	}

	ms := advmetricsset.New()

	client, err := queuedclient.Dial(ctx, queueconfig.New[staticConf](conf, testInstanceName), queuedclient.WithMetricsSet(ms))
	if err != nil {
		t.Fatal(err)
	}

	req := queuedproto.ReqGetActive{StorageType: testRawQueueID, Count: 1}

	// разогрев: обычный запрос доказывает работоспособность соединения
	// ("мягкие" ошибки типа "очередь ещё не существует" - это тоже успех транспорта)
	if _, _, err := client.MakeUnicastRequest(ctx, 0, req); err != nil && !queuedproto.IsSoftError(err) {
		t.Fatalf("warm-up request: %v", err)
	}

	// форсируем отмены запросов: заведомо истекающие дедлайны
	for range 50 {
		shortCtx, cancel := context.WithTimeout(ctx, 200*time.Microsecond)
		_, _, _ = client.MakeUnicastRequest(shortCtx, 0, req)

		cancel()
	}

	// даём отправителю/серверу время отреагировать, если они собирались
	time.Sleep(500 * time.Millisecond)

	dump := promDump(ms)

	if n := metricSum(dump, "iproto_cancels_sent_total"); n != 0 {
		t.Fatalf("client sent %v Cancel packets despite /cancels-enabled=false:\n%s", n, dump)
	}

	if n := metricSum(dump, "iproto_connections_closed_total"); n != 0 {
		t.Fatalf("connection was closed %v time(s) under request timeouts (the /cancels-enabled=false mitigation failed):\n%s", n, dump)
	}
}

// metricSum суммирует значения всех сэмплов метрики name в прометеусовском дампе
// (нулевые сэмплы зарегистрированных метрик тоже присутствуют в выводе).
func metricSum(dump, name string) float64 {
	var sum float64

	for line := range strings.Lines(dump) {
		if !strings.HasPrefix(line, name) {
			continue
		}

		rest := line[len(name):]
		if len(rest) == 0 || (rest[0] != '{' && rest[0] != ' ') {
			continue // другая метрика с этим префиксом
		}

		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}

		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err == nil {
			sum += v
		}
	}

	return sum
}

// rawCancelProbe: на чистом TCP выполняет валидный запрос, посылает Cancel (0xFF02)
// и проверяет, отвечает ли queued на следующий валидный запрос.
func rawCancelProbe(t *testing.T, addr string) (survived bool) {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("raw probe dial: %v", err)
	}

	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	const cmdGetActive = uint32(queuedproto.CmdGetActive)

	// ReqGetActive{StorageType, Count}: u16 + u32
	reqPayload := make([]byte, 6)
	binary.LittleEndian.PutUint16(reqPayload[0:], testRawQueueID)
	binary.LittleEndian.PutUint32(reqPayload[2:], 1)

	if _, err := conn.Write(rawPacket(cmdGetActive, 1, reqPayload)); err != nil {
		t.Fatalf("raw probe write: %v", err)
	}

	if _, err := readRawPacket(conn); err != nil {
		t.Fatalf("raw probe first response: %v", err)
	}

	cancelPayload := make([]byte, 4)
	binary.LittleEndian.PutUint32(cancelPayload, 12345) // несуществующий seq

	if _, err := conn.Write(rawPacket(0xFF02, 0, cancelPayload)); err != nil {
		t.Fatalf("raw probe write cancel: %v", err)
	}

	if _, err := conn.Write(rawPacket(cmdGetActive, 2, reqPayload)); err != nil {
		return false
	}

	if _, err := readRawPacket(conn); err != nil {
		return false
	}

	return true
}

func rawPacket(typ, seq uint32, payload []byte) []byte {
	b := make([]byte, 12+len(payload))
	binary.LittleEndian.PutUint32(b[0:], typ)
	binary.LittleEndian.PutUint32(b[4:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(b[8:], seq)
	copy(b[12:], payload)

	return b
}

func readRawPacket(conn net.Conn) ([]byte, error) {
	header := make([]byte, 12)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}

	payload := make([]byte, binary.LittleEndian.Uint32(header[4:]))
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, err
	}

	return payload, nil
}

func promDump(ms advmetricsset.Set) string {
	var buf bytes.Buffer

	ms.WritePrometheus(&buf)

	return buf.String()
}

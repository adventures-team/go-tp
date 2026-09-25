//go:build integration

package tptest

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	advmetricsset "github.com/adventures-team/go-adv-metrics/set"
	advslog "github.com/adventures-team/go-adv-slog"
	"github.com/adventures-team/go-queuedproto"
	"github.com/onlineconf/onlineconf-go/v2"

	"github.com/adventures-team/go-tp/queueconfig"
	"github.com/adventures-team/go-tp/queuedclient"
)

// TestHotReconnect проверяет горячее переподключение по цепочке целиком: подмена файла
// модуля onlineconf (fsnotify -> reopen -> подписка на /queueds/multi) -> адаптер
// iprotoSource -> пересчёт топологии в go-iprotoconn -> Apply: соединения с новыми
// адресами открываются (/eager-dial), запросы продолжают проходить без пересоздания клиента.
//
// Новые адреса - те же контейнеры queued под сетевыми алиасами (testShard*MovedAddr,
// networks.default.aliases в dev-env/docker-compose.yaml); алиасы не используются другими
// тестами, чтобы соединения (go-iprotoconn дедуплицирует их по строке адреса process-wide)
// и метрики были свои.
func TestHotReconnect(t *testing.T) {
	ctx := advslog.NewContext(context.Background(), advslog.InitTest(t))

	cdb := testCDB(t)

	conf, err := onlineconf.OpenSubtree(cdb, testConfPrefix)
	if err != nil {
		t.Fatal(err)
	}

	ms := advmetricsset.New()

	client, err := queuedclient.Dial(ctx, queueconfig.New(conf, testInstanceName), queuedclient.WithMetricsSet(ms))
	if err != nil {
		t.Fatal(err)
	}

	req := queuedproto.ReqGetActive{StorageType: testRawQueueID, Count: 1}

	// "мягкие" ошибки типа "очередь ещё не существует" - это тоже успех транспорта
	checkRequest := func(stage string) {
		t.Helper()

		if _, _, err := client.MakeUnicastRequest(ctx, 0, req); err != nil && !queuedproto.IsSoftError(err) {
			t.Fatalf("%s: request failed: %v", stage, err)
		}
	}

	checkRequest("before the switch")

	replaceServers(t, cdb, testShard0MovedAddr+","+testShard1MovedAddr)

	// onlineconf перечитывает модуль асинхронно (fsnotify), go-iprotoconn применяет топологию
	// в фоне: ждём, пока метрики покажут dial к обоим новым адресам
	waitServerMetric(t, ms, "iproto_dials_total", testShard0MovedAddr)
	waitServerMetric(t, ms, "iproto_dials_total", testShard1MovedAddr)

	checkRequest("after the switch")
}

// waitServerMetric ждёт появления ненулевого сэмпла метрики name с лейблом server=addr.
func waitServerMetric(t *testing.T, ms advmetricsset.Set, name, addr string) {
	t.Helper()

	const timeout = 15 * time.Second

	label := `server="` + addr + `"`
	deadline := time.Now().Add(timeout)

	for {
		dump := promDump(ms)

		if serverMetric(dump, name, label) > 0 {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("%s{%s} did not appear within %v:\n%s", name, label, timeout, dump)
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// serverMetric возвращает значение сэмпла метрики name, содержащего лейбл label (0, если его нет).
func serverMetric(dump, name, label string) float64 {
	for line := range strings.Lines(dump) {
		if !strings.HasPrefix(line, name+"{") || !strings.Contains(line, label) {
			continue
		}

		fields := strings.Fields(line)

		if v, err := strconv.ParseFloat(fields[len(fields)-1], 64); err == nil {
			return v
		}
	}

	return 0
}

var serversLineRE = regexp.MustCompile(`(?m)^(\s*multi:).*$`)

// replaceServers пересобирает модуль cdb с другим значением /queueds/multi и атомарно
// подменяет файл (rename в том же каталоге) - так же, как это делает onlineconf-updater:
// onlineconf-go следит за каталогом через fsnotify и перечитывает модуль по событию Create.
func replaceServers(t *testing.T, cdb, servers string) {
	t.Helper()

	src, err := os.ReadFile(fixtureYAML())
	if err != nil {
		t.Fatal(err)
	}

	if !serversLineRE.Match(src) {
		t.Fatalf("no queueds/multi line in %s", fixtureYAML())
	}

	yamlPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(yamlPath, serversLineRE.ReplaceAll(src, []byte("${1} "+servers)), 0o600); err != nil {
		t.Fatal(err)
	}

	tmp := cdb + ".tmp"
	buildCDB(t, yamlPath, tmp)

	if err := os.Rename(tmp, cdb); err != nil {
		t.Fatal(err)
	}
}

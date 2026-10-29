//go:build integration

package tptest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	advslog "github.com/adventures-team/go-adv-slog"
	"github.com/adventures-team/go-queuedproto"
	"github.com/google/go-cmp/cmp"
	"github.com/onlineconf/onlineconf-go/v2"
	"gopkg.in/yaml.v3"

	"github.com/adventures-team/go-tp/internal/mover"
	"github.com/adventures-team/go-tp/queueconfig"
	"github.com/adventures-team/go-tp/queuedclient"
)

// Тесты queued-mover (internal/mover) на dev-env: кластер A - queued_shard0/1, кластер B -
// queued_dst0/1 (dev-env/docker-compose.yaml). Конфиг - поддерево queued-mover из
// build/local/config.yaml с переопределениями теста.

const (
	moverSubtree    = "/queued-mover"
	moverRunTimeout = 90 * time.Second // с запасом: queued активирует новые события периодической процедурой
	moverWaitEvents = 60 * time.Second
	moverDeferDelay = 3 * time.Second
)

// moverTest - окружение теста: модуль onlineconf и клиенты обоих кластеров.
type moverTest struct {
	ctx      context.Context
	logger   *slog.Logger
	module   *onlineconf.Module
	src, dst *queuedclient.QueuedClient
}

func newMoverTest(t *testing.T, overrides map[string]any) *moverTest {
	t.Helper()

	logger := advslog.InitTest(t)
	ctx := advslog.NewContext(context.Background(), logger)
	module := moverModule(t, overrides)

	mt := &moverTest{
		ctx:    ctx,
		logger: logger,
		module: module,
		src:    dialSide(ctx, t, module, "src"),
		dst:    dialSide(ctx, t, module, "dst"),
	}

	// остатки предыдущих (сфейлившихся) запусков
	drainQueue(ctx, t, mt.src, testMoverQueueID)
	drainQueue(ctx, t, mt.dst, testMoverQueueID)

	return mt
}

// moverModule собирает onlineconf-модуль из поддерева queued-mover в build/local/config.yaml
// с переопределениями параметров.
func moverModule(t *testing.T, overrides map[string]any) *onlineconf.Module {
	t.Helper()

	raw, err := os.ReadFile(fixtureYAML())
	if err != nil {
		t.Fatal(err)
	}

	var doc map[string]any

	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("%s: %v", fixtureYAML(), err)
	}

	subtree, ok := doc["queued-mover"].(map[string]any)
	if !ok {
		t.Fatalf("%s: no queued-mover subtree", fixtureYAML())
	}

	for key, value := range overrides {
		subtree[key] = value
	}

	out, err := yaml.Marshal(map[string]any{"queued-mover": subtree})
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "mover.yaml")

	if err := os.WriteFile(yamlPath, out, 0o600); err != nil {
		t.Fatal(err)
	}

	cdb := filepath.Join(dir, "TREE.cdb")
	buildCDB(t, yamlPath, cdb)

	module, err := onlineconf.OpenModule(cdb)
	if err != nil {
		t.Fatal(err)
	}

	return module
}

// dialSide создаёт клиент кластера side (src или dst) так же, как это делает cmd/queued-mover.
func dialSide(ctx context.Context, t *testing.T, module *onlineconf.Module, side string) *queuedclient.QueuedClient {
	t.Helper()

	client, err := queuedclient.Dial(ctx, queueconfig.New(module.Subtree(moverSubtree+"/"+side), testInstanceName))
	if err != nil {
		t.Fatalf("Dial(%s): %v", side, err)
	}

	return client
}

// run запускает перенос с таймаутом и возвращает результат Mover.Run.
func (mt *moverTest) run(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(mt.ctx, timeout)
	defer cancel()

	return mover.New(mt.module.Subtree(moverSubtree), mt.src, mt.dst, mt.logger).Run(ctx)
}

// assertSourceEmpty проверяет по статистике, что в очереди A не осталось событий.
func (mt *moverTest) assertSourceEmpty(t *testing.T) {
	t.Helper()

	items, active, locked := sumQueueStat(mt.ctx, t, mt.src, testMoverQueueID)
	if items != 0 {
		t.Errorf("source queue is not empty: items %d, active %d, locked %d", items, active, locked)
	}
}

// publishEvents публикует n событий с телами prefix-i в очередь кластера client; eventID
// задаёт ID i-го события (шард - BalanceShard, ID 0 - назначает queued). Возвращает тела по ID.
func publishEvents(ctx context.Context, t *testing.T, client *queuedclient.QueuedClient, n int, prefix string,
	eventID func(i int) uint64) map[queuedproto.EventID][]byte {
	t.Helper()

	published := make(map[queuedproto.EventID][]byte, n)

	for i := range n {
		data := []byte(fmt.Sprintf("%s-%d", prefix, i))

		id, err := client.Publish(ctx, testMoverQueueID, queuedproto.EventID{Shard: queuedclient.BalanceShard, ID: eventID(i)}, 0, data)
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}

		published[id] = data
	}

	return published
}

func autoID(int) uint64 { return 0 }

// collectEvents выгребает из очереди кластера client ровно want событий (ждёт их активации),
// удаляя их, и возвращает тела.
func collectEvents(ctx context.Context, t *testing.T, client *queuedclient.QueuedClient, want int, timeout time.Duration) [][]byte {
	t.Helper()

	deadline := time.Now().Add(timeout)
	payloads := make([][]byte, 0, want)

	for len(payloads) < want {
		if time.Now().After(deadline) {
			t.Fatalf("collected %d of %d events before the deadline", len(payloads), want)
		}

		events, err := client.GetActive(ctx, testMoverQueueID, want)
		if err != nil && len(events.Items) == 0 {
			t.Fatalf("GetActive: %v", err)
		}

		if len(events.Items) == 0 {
			time.Sleep(testPollDelay / 5)
			continue
		}

		ids := make([]queuedproto.EventID, 0, len(events.Items))

		for _, event := range events.Items {
			payloads = append(payloads, event.Data)
			ids = append(ids, event.EventID)
		}

		if err := client.Delete(ctx, testMoverQueueID, ids); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}

	if len(payloads) > want {
		t.Fatalf("collected %d events, want %d", len(payloads), want)
	}

	return payloads
}

// sortedStrings - тела событий строками по порядку, для сравнения множеств.
func sortedStrings(payloads ...[]byte) []string {
	strs := make([]string, len(payloads))

	for i, data := range payloads {
		strs[i] = string(data)
	}

	sort.Strings(strs)

	return strs
}

func mapValues(m map[queuedproto.EventID][]byte) [][]byte {
	values := make([][]byte, 0, len(m))

	for _, data := range m {
		values = append(values, data)
	}

	return values
}

// TestMoverNewIDs - перенос с новыми ID в режиме one-shot: все события (включая отложенные,
// которые переносятся после активации) оказываются в B с теми же телами, A пуст.
func TestMoverNewIDs(t *testing.T) {
	mt := newMoverTest(t, nil)
	published := publishEvents(mt.ctx, t, mt.src, 12, "new", autoID)

	deferred := make([]queuedproto.EventID, 0, 4)

	for id := range published {
		if len(deferred) == cap(deferred) {
			break
		}

		deferred = append(deferred, id)
	}

	if err := mt.src.Defer(mt.ctx, testMoverQueueID, moverDeferDelay, deferred); err != nil {
		t.Fatalf("Defer: %v", err)
	}

	if err := mt.run(moverRunTimeout); err != nil {
		t.Fatalf("Run: %v", err)
	}

	mt.assertSourceEmpty(t)

	got := collectEvents(mt.ctx, t, mt.dst, len(published), moverWaitEvents)

	if diff := cmp.Diff(sortedStrings(mapValues(published)...), sortedStrings(got...)); diff != "" {
		t.Errorf("payloads in the destination mismatch (-want +got):\n%s", diff)
	}
}

// TestMoverPreserveIDs - перенос с сохранением ID: события лежат в B под теми же ID в шардах
// по CalcShard, как их положил бы tp.Queue.Publish.
func TestMoverPreserveIDs(t *testing.T) {
	mt := newMoverTest(t, map[string]any{"preserve_ids": 1})
	published := publishEvents(mt.ctx, t, mt.src, 6, "keep", func(i int) uint64 { return 3_000_001 + uint64(i) })

	if err := mt.run(moverRunTimeout); err != nil {
		t.Fatalf("Run: %v", err)
	}

	mt.assertSourceEmpty(t)

	dstIDs := make([]queuedproto.EventID, 0, len(published))

	for srcID, data := range published {
		dstID := queuedproto.EventID{ID: srcID.ID, Shard: queuedclient.CalcShard(srcID.ID, mt.dst.ShardNum())}
		dstIDs = append(dstIDs, dstID)

		events, err := mt.dst.GetByIDs(mt.ctx, testMoverQueueID, []queuedproto.EventID{dstID})
		if err != nil {
			t.Fatalf("GetByIDs(%v): %v", dstID, err)
		}

		if len(events.Items) != 1 || string(events.Items[0].Data) != string(data) {
			t.Errorf("event %v in the destination: %v, want one event with payload %q", dstID, events.Items, data)
		}
	}

	if err := mt.dst.Delete(mt.ctx, testMoverQueueID, dstIDs); err != nil {
		t.Errorf("cleanup Delete: %v", err)
	}
}

// TestMoverDuplicateAndCollision - сохранение ID, когда в B уже есть события с такими ID:
// с тем же телом - дубль (удаляется из A), с другим - коллизия (остаётся в A, one-shot
// не завершается).
func TestMoverDuplicateAndCollision(t *testing.T) {
	mt := newMoverTest(t, map[string]any{"preserve_ids": 1})

	const (
		dupID = uint64(4_000_001)
		colID = uint64(4_000_002)
	)

	dstID := func(id uint64) queuedproto.EventID {
		return queuedproto.EventID{ID: id, Shard: queuedclient.CalcShard(id, mt.dst.ShardNum())}
	}

	for _, event := range []struct {
		id   uint64
		data string
	}{{dupID, "same"}, {colID, "other"}} {
		if _, err := mt.dst.Publish(mt.ctx, testMoverQueueID, dstID(event.id), 0, []byte(event.data)); err != nil {
			t.Fatalf("Publish into the destination: %v", err)
		}
	}

	srcIDs := make(map[uint64]queuedproto.EventID, 2)

	for _, event := range []struct {
		id   uint64
		data string
	}{{dupID, "same"}, {colID, "different"}} {
		id, err := mt.src.Publish(mt.ctx, testMoverQueueID, queuedproto.EventID{Shard: queuedclient.BalanceShard, ID: event.id}, 0, []byte(event.data))
		if err != nil {
			t.Fatalf("Publish into the source: %v", err)
		}

		srcIDs[event.id] = id
	}

	defer func() {
		if err := mt.src.Delete(mt.ctx, testMoverQueueID, []queuedproto.EventID{srcIDs[colID]}); err != nil {
			t.Errorf("cleanup Delete from the source: %v", err)
		}

		if err := mt.dst.Delete(mt.ctx, testMoverQueueID, []queuedproto.EventID{dstID(dupID), dstID(colID)}); err != nil {
			t.Errorf("cleanup Delete from the destination: %v", err)
		}
	}()

	err := mt.run(20 * time.Second)
	if !errors.Is(err, mover.ErrInterrupted) {
		t.Fatalf("Run: %v, want ErrInterrupted (the collision stays in the source)", err)
	}

	if items, _, _ := sumQueueStat(mt.ctx, t, mt.src, testMoverQueueID); items != 1 {
		t.Errorf("source queue: %d items, want 1 (the collision)", items)
	}

	for id, want := range map[uint64]string{dupID: "same", colID: "other"} {
		events, err := mt.dst.GetByIDs(mt.ctx, testMoverQueueID, []queuedproto.EventID{dstID(id)})
		if err != nil {
			t.Fatalf("GetByIDs(%d): %v", id, err)
		}

		if len(events.Items) != 1 || string(events.Items[0].Data) != want {
			t.Errorf("event %d in the destination: %v, want one event with payload %q", id, events.Items, want)
		}
	}
}

// TestMoverService - режим сервиса: события, опубликованные в A после старта, появляются в B;
// остановка завершает Run без ошибки и не оставляет заблокированных событий в A.
func TestMoverService(t *testing.T) {
	mt := newMoverTest(t, map[string]any{"one_shot": 0})

	ctx, cancel := context.WithTimeout(mt.ctx, moverRunTimeout)
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- mover.New(mt.module.Subtree(moverSubtree), mt.src, mt.dst, mt.logger).Run(ctx)
	}()

	time.Sleep(testPollDelay) // сервис опрашивает A

	published := publishEvents(mt.ctx, t, mt.src, 3, "svc", autoID)
	got := collectEvents(mt.ctx, t, mt.dst, len(published), moverWaitEvents)

	if diff := cmp.Diff(sortedStrings(mapValues(published)...), sortedStrings(got...)); diff != "" {
		t.Errorf("payloads in the destination mismatch (-want +got):\n%s", diff)
	}

	cancel()

	if err := <-done; err != nil {
		t.Errorf("Run after stop: %v, want nil", err)
	}

	mt.assertSourceEmpty(t)
}

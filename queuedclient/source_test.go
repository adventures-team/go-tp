package queuedclient

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/adventures-team/go-iprotoconn/cluster"
	"github.com/adventures-team/go-iprotoconn/config"
	"github.com/google/go-cmp/cmp"
	"github.com/onlineconf/onlineconf-go/v2"

	"gitlab.adv.team/go/tp.git/queueconfig"
)

// subscribableConf - минимальный queueconfig.OnlineConf с подписками в духе onlineconf-go:
// хранит только /queueds/multi и уведомляет подписчиков при его смене.
type subscribableConf struct {
	mu      sync.Mutex
	servers string
	subs    map[string]map[chan<- struct{}]struct{} // путь -> подписанные каналы
}

func newSubscribableConf(servers string) *subscribableConf {
	return &subscribableConf{servers: servers, subs: map[string]map[chan<- struct{}]struct{}{}}
}

func (c *subscribableConf) GetString(path, dflt string) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	if path == ConfServers {
		return c.servers
	}

	return dflt
}

func (*subscribableConf) GetStringIfExists(string) (string, bool) { return "", false }
func (*subscribableConf) GetInt(_ string, dflt int) int           { return dflt }
func (*subscribableConf) GetIntErr(string) (int, error)           { return 0, onlineconf.ErrNotFound }
func (*subscribableConf) Subscribe(string) (chan struct{}, error) { return make(chan struct{}, 1), nil }
func (c *subscribableConf) Subtree(string) *subscribableConf      { return c }

func (c *subscribableConf) SubscribeChan(path string, ch chan<- struct{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subs[path] == nil {
		c.subs[path] = map[chan<- struct{}]struct{}{}
	}

	c.subs[path][ch] = struct{}{}

	return nil
}

func (c *subscribableConf) UnsubscribeChan(path string, ch chan<- struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.subs[path], ch)

	if len(c.subs[path]) == 0 {
		delete(c.subs, path)
	}
}

// set меняет /queueds/multi и уведомляет подписчиков (неблокирующе, как onlineconf-go).
func (c *subscribableConf) set(servers string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.servers = servers

	for ch := range c.subs[ConfServers] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// subscriptions возвращает число подписанных каналов по путям.
func (c *subscribableConf) subscriptions() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make(map[string]int, len(c.subs))
	for path, chans := range c.subs {
		out[path] = len(chans)
	}

	return out
}

// masters строит топологию go-iprotoconn "один мастер на шард" - то, во что адаптер
// отображает /queueds/multi.
func masters(addrs ...string) cluster.Topology {
	var top cluster.Topology

	for _, addr := range addrs {
		top.Shards = append(top.Shards, cluster.ShardTopology{Masters: [][]string{{addr}}})
	}

	return top
}

// awaitTopology читает обновления, пока не придёт want (или не истечёт таймаут).
// Промежуточные обновления допустимы: go-iprotoconn собирает топологию несколькими
// вызовами GetStrings, и при смене конфига прямо во время пересчёта может увидеть
// смешанный список - но следующим же уведомлением сходится к актуальному. Число шардов
// при этом всегда зафиксированное - это проверяется у каждого обновления.
func awaitTopology(t *testing.T, updates <-chan cluster.Topology, want cluster.Topology) {
	t.Helper()

	deadline := time.After(5 * time.Second)

	for {
		select {
		case got, ok := <-updates:
			if !ok {
				t.Fatal("topology updates channel is closed")
			}

			if len(got.Shards) != len(want.Shards) {
				t.Fatalf("emitted topology has %d shards, want %d (the shard count must stay pinned): %+v",
					len(got.Shards), len(want.Shards), got)
			}

			if cmp.Equal(want, got) {
				return
			}
		case <-deadline:
			t.Fatalf("topology %+v was not emitted", want)
		}
	}
}

// TestIprotoSourceHotReconnect прогоняет адаптер через настоящий наблюдатель топологии
// go-iprotoconn (config.NewTopologyProvider(...).Watch) - именно он решает, подписываем
// ли источник, и именно его вывод уходит в Cluster.Apply.
func TestIprotoSourceHotReconnect(t *testing.T) {
	conf := newSubscribableConf("127.0.0.1:1001, 127.0.0.1:1002")

	src, err := newIprotoSource(context.Background(), queueconfig.New(conf, "test"))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// nil здесь означал бы "hot reconnect disabled" с предупреждением при старте
	updates := config.NewTopologyProvider(src).Watch(ctx)
	if updates == nil {
		t.Fatal("Watch returned nil: iprotoSource is not seen as config.Subscribable")
	}

	// три виртуальных пути (/shards, /masters, /replicas) сворачиваются в одну подписку
	// на реальный ключ /queueds/multi
	if diff := cmp.Diff(map[string]int{ConfServers: 1}, conf.subscriptions()); diff != "" {
		t.Errorf("subscriptions mismatch (-want +got):\n%s", diff)
	}

	// смена адресов при том же числе шардов применяется
	conf.set("127.0.0.1:2001,127.0.0.1:2002")
	awaitTopology(t, updates, masters("127.0.0.1:2001", "127.0.0.1:2002"))

	// другое число шардов отвергается: список остаётся прежним, топология не меняется
	conf.set("127.0.0.1:3001")

	if diff := cmp.Diff([]string{"127.0.0.1:2001", "127.0.0.1:2002"}, src.servers()); diff != "" {
		t.Errorf("servers after a rejected shard count change (-want +got):\n%s", diff)
	}

	// следующее валидное изменение применяется; отвергнутое (один шард) не должно
	// проявиться ни в одном обновлении
	conf.set("127.0.0.1:4001,127.0.0.1:4002")
	awaitTopology(t, updates, masters("127.0.0.1:4001", "127.0.0.1:4002"))

	// остановка наблюдателя снимает подписку (go-iprotoconn отписывается до закрытия канала)
	cancel()

	select {
	case _, ok := <-updates:
		if ok {
			t.Fatal("unexpected topology update after cancel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("updates channel is not closed after cancel")
	}

	if diff := cmp.Diff(map[string]int{}, conf.subscriptions()); diff != "" {
		t.Errorf("subscriptions after cancel (-want +got):\n%s", diff)
	}
}

func TestIprotoSourceNoServers(t *testing.T) {
	_, err := newIprotoSource(context.Background(), queueconfig.New(newSubscribableConf(" , "), "test"))
	if !errors.Is(err, ErrNoServers) {
		t.Fatalf("got %v, want ErrNoServers", err)
	}
}

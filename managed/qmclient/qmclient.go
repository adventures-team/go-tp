package qmclient

import (
	"context"
	"time"

	advslog "github.com/adventures-team/go-adv-slog"
	iprotoconn "github.com/adventures-team/go-iprotoconn"
	"github.com/adventures-team/go-iprotoconn/config"
)

const (
	cmdStat     = 2
	cmdSetQueue = 3

	answerOK   = 0
	answerFail = 4

	cluster = "generic"
)

// StatsRequest - запрос статистики от менеджера очередей. Тело запроса игнорируется
// (историческое поведение перлового протокола).
type StatsRequest struct{}

// Cmd возвращает код команды для типизированного мультиплексора go-iprotoconn.
func (StatsRequest) Cmd() uint32 { return cmdStat }

// UnmarshalIProto игнорирует тело запроса.
func (*StatsRequest) UnmarshalIProto(buf []byte) ([]byte, error) { return buf, nil }

// Cmd возвращает код команды для типизированного мультиплексора go-iprotoconn.
func (SetQueueRequest) Cmd() uint32 { return cmdSetQueue }

//adv:iproto:
type StatsResponse struct {
	Status  uint32
	Data    StatsData
	Cluster string `iproto:"u32"`
}

type StatsData struct {
	PID                   uint32
	StartedAt             uint32
	LastSlotStatUpdatedAt uint32
	TotalSlots            uint32
	UnknownSlots          uint32
	AdminConsolePort      uint32
	Slots                 []SlotStat `iproto:"u32"`
}

type SlotStat struct {
	UpdateTime     uint32
	StartTime      uint32
	Status         uint32
	PID            uint32
	SlotNumber     uint32
	State          uint32
	LoadErrors     uint32
	Switches       uint32
	Interval       float64
	Overall        QueueStat
	QueueStat      QueueExtendedStat
	NextQueueID    uint32
	NextQueueFlags uint32
	ArchivedQueues []QueueExtendedStat `iproto:"u32"`
}

type QueueExtendedStat struct {
	QueueID   uint32
	Flags     uint32
	StartedAt uint32
	StoppedAt uint32
	Current   QueueStat
	Total     QueueStat
}

type QueueStat struct {
	EvalErrors uint32
	Processed  uint32
	Errors     uint32
	Loops      uint32
	WorkTime   float64
	WaitTime   float64
	Started    uint32
	Stopped    uint32
}

//adv:iproto:
type SetQueueRequest struct {
	Queues []SetQueue `iproto:"u32"`
}

type SetQueue struct {
	Slot    uint32
	QueueID uint32
	Flags   uint32
}

//adv:iproto:
type SetQueueResponse struct {
	Status           uint32
	SetQueueStatuses []uint32 `iproto:"u32"`
	Stats            StatsResponse
}

type Client struct {
	endpoint   string
	assignSlot func(slotID, queueID uint32) error
	stats      func() StatsData
}

func NewClient(endpoint string, assignSlot func(slotID, queueID uint32) error, stats func() StatsData) *Client {
	return &Client{
		endpoint:   endpoint,
		assignSlot: assignSlot,
		stats:      stats,
	}
}

// qmSource - статический config.Source для go-iprotoconn: один "шард" с одним
// мастером (менеджер очередей); соединение поддерживается постоянно (/eager-dial),
// Cancel-пакеты выключены (легаси-пир закрывает соединение на неизвестных типах
// сообщений - проверено на реальном queued, см. TestCancelDoesNotKillConnection).
// Остальные параметры - дефолты go-iprotoconn.
type qmSource struct {
	endpoint string
}

func (s qmSource) GetStrings(path string, dflt []string) []string {
	if path == "/masters" {
		return []string{s.endpoint}
	}

	return dflt
}

func (qmSource) GetBool(path string, dflt bool) bool {
	switch path {
	case "/eager-dial", "/redial-forever":
		return true
	case "/cancels-enabled":
		return false
	}

	return dflt
}

func (qmSource) GetString(_ string, dflt string) string                 { return dflt }
func (qmSource) GetStringIfExists(_ string) (string, bool)              { return "", false }
func (qmSource) GetInt(_ string, dflt int) int                          { return dflt }
func (qmSource) GetDuration(_ string, dflt time.Duration) time.Duration { return dflt }
func (qmSource) GetFloat(_ string, dflt float64) float64                { return dflt }

// Подписки (config.Subscribable): адрес менеджера очередей фиксирован на всё время жизни
// клиента - меняться нечему, поэтому подписки принимаются, но никогда не срабатывают.
// Без них go-iprotoconn при каждом запуске предупреждал бы о выключенном горячем
// переподключении, что здесь бессмысленно.
func (qmSource) SubscribeChan(string, chan<- struct{}) error        { return nil }
func (qmSource) UnsubscribeChan(string, chan<- struct{})            {}
func (qmSource) SubscribeChanSubtree(string, chan<- struct{}) error { return nil }
func (qmSource) UnsubscribeChanSubtree(string, chan<- struct{})     {}

var _ config.Subscribable = qmSource{}

// Run поддерживает соединение с менеджером очередей и обслуживает его команды
// (обратный RPC: соединение устанавливает клиент, запросы шлёт менеджер).
// Возврат - по отмене ctx.
func (c *Client) Run(ctx context.Context) {
	// отмена ctx для go-iprotoconn означает мгновенный сброс соединений;
	// нам нужен graceful shutdown - поэтому фоновые механизмы клиента
	// отвязываются от ctx, а по ctx.Done() зовётся Close.
	client, err := iprotoconn.NewClient(context.WithoutCancel(ctx), qmSource{endpoint: c.endpoint},
		iprotoconn.WithLogger(advslog.Ctx(ctx)))
	if err != nil {
		advslog.FatalContext(ctx, "qmclient: iproto client", advslog.Err(err))
	}

	mux := iprotoconn.NewMux()
	iprotoconn.MustHandle(mux, c.handleStats)
	iprotoconn.MustHandle(mux, c.handleSetQueue)

	// Handle сразу после NewClient: соединение поднимается в фоне (/eager-dial),
	// и пуш, пришедший до подключения мультиплексора, был бы отброшен.
	// Менеджер очередей опрашивает клиентов периодически, поэтому микросекундное
	// окно здесь безвредно.
	if err := client.Handle(mux); err != nil {
		advslog.FatalContext(ctx, "qmclient: attach mux", advslog.Err(err))
	}

	<-ctx.Done()

	_ = client.Close(context.Background())
}

func (c *Client) handleStats(_ context.Context, _ *StatsRequest) *StatsResponse {
	return &StatsResponse{
		Status:  answerOK,
		Data:    c.stats(),
		Cluster: cluster,
	}
}

func (c *Client) handleSetQueue(_ context.Context, req *SetQueueRequest) *SetQueueResponse {
	queueStatuses := make([]uint32, 0, len(req.Queues))

	for _, j := range req.Queues {
		if err := c.assignSlot(j.Slot, j.QueueID); err != nil {
			queueStatuses = append(queueStatuses, answerFail)
		} else {
			queueStatuses = append(queueStatuses, answerOK)
		}
	}

	return &SetQueueResponse{
		Status:           answerOK,
		SetQueueStatuses: queueStatuses,
		Stats: StatsResponse{
			Status:  answerOK,
			Data:    c.stats(),
			Cluster: cluster,
		},
	}
}

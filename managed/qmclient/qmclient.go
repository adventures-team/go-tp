package qmclient

import (
	"context"
	"net"
	"time"

	advslog "github.com/adventures-team/go-adv-slog"

	easygo "gitlab.adv.team/go/easygo.git/iproto"
	"gitlab.adv.team/go/tp.git/internal"
	"gitlab.adv.team/go/tp.git/internal/iprotolog"
)

const (
	cmdStat      = 2
	cmdSetQueue  = 3
	cmdBroadcast = 10

	answerOK   = 0
	answerFail = 4

	cluster = "generic"
)

type Connection interface {
	RemoteAddr() net.Addr
	LocalAddr() net.Addr
	Call(ctx context.Context, message uint32, data []byte) ([]byte, error)
	Notify(ctx context.Context, message uint32, data []byte) error
	Send(ctx context.Context, packet easygo.Packet) error
	Close()
	Shutdown()
	Done() <-chan struct{}
	OnClose(func())
	GetBytes(n int) []byte
	PutBytes(p []byte)
}

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
	endpoint       string
	timeout        time.Duration
	redialInterval time.Duration
	assignSlot     func(slotID, queueID uint32) error
	stats          func() StatsData
}

func NewClient(endpoint string, assignSlot func(slotID, queueID uint32) error, stats func() StatsData) *Client {
	return &Client{
		endpoint:   endpoint,
		assignSlot: assignSlot,
		stats:      stats,
	}
}

func (c *Client) Run(ctx context.Context) {
	logAdapter := iprotolog.Logger{}

	pool, err := easygo.Dial(ctx, "tcp", c.endpoint, &easygo.PoolConfig{
		Size:              1,
		ConnectTimeout:    c.timeout,
		DialTimeout:       c.timeout,
		RedialInterval:    c.redialInterval,
		MaxRedialInterval: c.redialInterval,
		RedialForever:     true,
		Logger:            logAdapter,
		ChannelConfig: &easygo.ChannelConfig{
			Handler:        c,
			WriteTimeout:   c.timeout,
			RequestTimeout: c.timeout,
			Logger:         logAdapter,
		},
	})
	if err != nil {
		advslog.FatalContext(ctx, "qmclient: iproto dial", advslog.Err(err))
	}

	<-ctx.Done()
	pool.Close()
}

func (c *Client) ServeIProto(ctx context.Context, conn easygo.Conn, packet easygo.Packet) {
	advslog.Ctx(ctx).Debug("command received", "msg", packet.Header.Msg)

	switch packet.Header.Msg {
	case cmdStat:
		response := StatsResponse{
			Status:  answerOK,
			Data:    c.stats(),
			Cluster: cluster,
		}

		data, err := response.MarshalIProto(internal.AllocBytes())
		if err != nil {
			advslog.Ctx(ctx).Error("marshal stats response", advslog.Err(err))
			break
		}

		err = conn.Send(ctx, easygo.Packet{
			Header: easygo.Header{
				Msg:  packet.Header.Msg,
				Len:  uint32(len(data)),
				Sync: packet.Header.Sync,
			},
			Data: data,
		})
		if err != nil {
			advslog.Ctx(ctx).Error("send stats response", advslog.Err(err))
		}
	case cmdSetQueue:
		var req SetQueueRequest

		_, err := req.UnmarshalIProto(packet.Data)
		if err != nil {
			advslog.Ctx(ctx).Error("unmarshal set-queue request", advslog.Err(err))
			break // TODO AnswerFail should be sent
		}

		queueStatuses := make([]uint32, 0, len(req.Queues))

		for _, j := range req.Queues {
			if err = c.assignSlot(j.Slot, j.QueueID); err != nil {
				queueStatuses = append(queueStatuses, answerFail)
			} else {
				queueStatuses = append(queueStatuses, answerOK)
			}
		}

		data, err := SetQueueResponse{
			Status:           answerOK,
			SetQueueStatuses: queueStatuses,
			Stats: StatsResponse{
				Status:  answerOK,
				Data:    c.stats(),
				Cluster: cluster,
			},
		}.MarshalIProto(internal.AllocBytes())
		if err != nil {
			advslog.Ctx(ctx).Error("marshal set-queue response", advslog.Err(err))
			break
		}

		err = conn.Send(ctx, easygo.Packet{
			Header: easygo.Header{
				Msg:  packet.Header.Msg,
				Len:  uint32(len(data)),
				Sync: packet.Header.Sync,
			},
			Data: data,
		})
		if err != nil {
			advslog.Ctx(ctx).Error("send set-queue response", advslog.Err(err))
		}
	default:
		advslog.Ctx(ctx).Error("unexpected msg", "msg", packet.Header.Msg)
	}
}

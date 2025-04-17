package qmclient

import (
	"context"
	"net"
	"time"

	"github.com/rs/zerolog/log"

	easygo "gitlab.cebc.tools/go/easygo.git/iproto"
	"gitlab.cebc.tools/go/tp.git/internal"
	"gitlab.cebc.tools/go/utils.git/mylog"
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
	logAdapter := mylog.IprotoLogger{}

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
		log.Fatal().Err(err).Send()
	}

	<-ctx.Done()
	pool.Close()
}

func (c *Client) ServeIProto(ctx context.Context, conn easygo.Conn, packet easygo.Packet) {
	log.Debug().Uint32("msg", packet.Header.Msg).Msg("command received")

	switch packet.Header.Msg {
	case cmdStat:
		response := StatsResponse{
			Status:  answerOK,
			Data:    c.stats(),
			Cluster: cluster,
		}

		data, err := response.MarshalIProto(internal.AllocBytes())
		if err != nil {
			log.Error().Err(err).Send()
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
			log.Error().Err(err).Send()
		}
	case cmdSetQueue:
		var req SetQueueRequest

		_, err := req.UnmarshalIProto(packet.Data)
		if err != nil {
			log.Error().Err(err).Send()
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
			log.Error().Err(err).Send()
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
			log.Error().Err(err).Send()
		}
	default:
		log.Error().Msgf("unexpected msg: %d", packet.Header.Msg)
	}
}

package qmclient

import (
	"context"
	"testing"

	"github.com/go-faker/faker/v4"
	"go.uber.org/mock/gomock"

	easygo "gitlab.adv.team/go/easygo.git/iproto"
	"gitlab.adv.team/go/tp.git/internal"
	mockiproto "gitlab.adv.team/go/tp.git/test/mock/iproto"
)

func TestServeIProto_cmdStat(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		conn := mockiproto.NewMockConnection(ctrl)

		data := StatsData{}
		_ = faker.FakeData(&data)

		collectStatsData := func() StatsData {
			return data
		}
		client := Client{
			stats: collectStatsData,
		}
		req := easygo.Packet{
			Header: easygo.Header{
				Msg:  cmdStat,
				Sync: 1,
			},
		}

		statsResp := StatsResponse{
			Status:  answerOK,
			Data:    data,
			Cluster: "generic",
		}

		iprotoResp, err := statsResp.MarshalIProto(internal.AllocBytes())
		if err != nil {
			t.Fatal("statsResp.MarshalIProto: ", err)
		}

		conn.EXPECT().Send(ctx, easygo.Packet{
			Header: easygo.Header{
				Msg:  req.Header.Msg,
				Len:  uint32(len(iprotoResp)),
				Sync: req.Header.Sync,
			},
			Data: iprotoResp,
		})

		client.ServeIProto(ctx, conn, req)
	})
}

func TestServeIProto_cmdSetQueue(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		conn := mockiproto.NewMockConnection(ctrl)

		assignSlotCalls := 0
		data := StatsData{}
		// _ = faker.FakeData(&data)

		collectStatsData := func() StatsData {
			return data
		}
		assignSlot := func(uint32, uint32) error {
			assignSlotCalls++
			return nil
		}
		client := Client{
			stats:      collectStatsData,
			assignSlot: assignSlot,
		}

		setQueues := []SetQueue{{Slot: 0, QueueID: 1}, {Slot: 1, QueueID: 2}}
		reqData, _ := SetQueueRequest{Queues: setQueues}.MarshalIProto(internal.AllocBytes())
		req := easygo.Packet{
			Header: easygo.Header{
				Msg:  cmdSetQueue,
				Sync: 1,
			},
			Data: reqData,
		}

		statsResp := StatsResponse{
			Status:  answerOK,
			Data:    data,
			Cluster: "generic",
		}
		queueResp := SetQueueResponse{
			Status:           answerOK,
			SetQueueStatuses: []uint32{answerOK, answerOK},
			Stats:            statsResp,
		}
		iprotoResp, _ := queueResp.MarshalIProto(internal.AllocBytes())
		_ = iprotoResp

		conn.EXPECT().Send(ctx, easygo.Packet{
			Header: easygo.Header{
				Msg:  req.Header.Msg,
				Len:  uint32(len(iprotoResp)),
				Sync: req.Header.Sync,
			},
			Data: iprotoResp,
		})

		client.ServeIProto(ctx, conn, req)

		got, want := assignSlotCalls, 2
		if got != want {
			t.Errorf("unexpected number of assignSlot calls: got %d, want %d", got, want)
		}
	})
}

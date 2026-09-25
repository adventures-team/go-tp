package tptest

import (
	"github.com/adventures-team/go-tp/queueconfig"
)

// OnlineConfWithSubtree is for test use only.
type OnlineConfWithSubtree interface {
	queueconfig.OnlineConfWithSubtree[queueconfig.OnlineConf]
}

package tptest

import (
	"gitlab.adv.team/go/tp.git/queueconfig"
)

// OnlineConfWithSubtree is for test use only.
type OnlineConfWithSubtree interface {
	queueconfig.OnlineConfWithSubtree[queueconfig.OnlineConf]
}

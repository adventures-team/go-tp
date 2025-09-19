package tptest

import (
	"gitlab.cebc.tools/go/tp.git/queueconfig"
)

// OnlineConfWithSubtree is for test use only.
type OnlineConfWithSubtree interface {
	queueconfig.OnlineConfWithSubtree[queueconfig.OnlineConf]
}

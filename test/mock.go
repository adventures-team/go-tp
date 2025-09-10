//go:build !gomarkdoc
// +build !gomarkdoc

//go:generate go tool mockgen -copyright_file build_tag -typed -destination onlineconf_mock_test.go -package $GOPACKAGE gitlab.cebc.tools/go/tp.git/queueconfig OnlineConf
//go:generate go tool mockgen -copyright_file build_tag -typed -destination onlineconf_subtree_mock_test.go -package $GOPACKAGE .  OnlineConfWithSubtree

// TODO https://github.com/golang/mock/issues/282
// переоткрыть/запостить новое issue - такой подход совместим лишь с устаревшим ＋build, а не с новыми go:build, которые идут без пробела.
// сейчас новый тэг автоматом вставляется goimports, но если это выпилят - будет нехорошо :)

package tptest

import (
	"gitlab.cebc.tools/go/tp.git/queueconfig"
)

type OnlineConfWithSubtree interface {
	queueconfig.OnlineConfWithSubtree[queueconfig.OnlineConf]
}

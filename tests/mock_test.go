//go:build !gomarkdoc
// +build !gomarkdoc

//go:generate mockgen -copyright_file build_tag -destination onlineconf_mock_test.go -package $GOPACKAGE gitlab.corp.mail.ru/mygo/utils.git/oconf OnlineConf

// TODO https://github.com/golang/mock/issues/282
// переоткрыть/запостить новое issue - такой подход совместим лишь с устаревшим ＋build, а не с новыми go:build, которые идут без пробела.
// сейчас новый тэг автоматом вставляется goimports, но если это выпилят - будет нехорошо :)

package tptest

import (
	_ "github.com/golang/mock/mockgen/model"
)

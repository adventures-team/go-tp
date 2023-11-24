//go:build integration || stress
// +build integration stress

package tptest

import "time"

const (
	testConfPrefix       = "/"
	testInstanceName     = "test"
	testRawQueueID       = 65534
	testAddonsQueueID    = 65533
	testNoHandlerQueueID = 65532
	testSleeperQueueID   = 65531
	testShardingQueueID  = 65530
	testBatchSize        = 10
	testDefer            = 5 * time.Second
	testPollDelay        = time.Second

	logLevelTrace = "trace"
	logLevelDebug = "debug"

	// настройки тестов на кол-во воркеров (TestDeferred, TestOOM, TestResize.
	// nolint, т.к. revive решил, что min - это minute. но на значение посмотрел бы )) тоже мне, искуственый интеллек )
	durationMin            = 3 * time.Second //nolint:revive
	durationMax            = 5 * time.Second
	publishDelay           = 100 * time.Millisecond
	publishDelayLong       = 500 * time.Millisecond        // чтобы хоть как-то размазало по воркерам, если не выставили размер пачки (TestDeferred)
	preTestDelay           = 5 * time.Second               // задержка после публикации (или после восстановления дефолтного кол-ва воркеров) перед прогоном тестов, чтобы устаканился пул воркеров
	workerPoolSustainDelay = 2*durationMax + time.Second/2 // 2 - это кол-во шардов - т.к. мы попросили пачку из 1 элемента, но приедут 2 их 2 шардов, + полсекунды на избежание рейс кондишенов. поменять при изменении кол-ва шардов. TODO сделать переменной, вычисляемой из строки из мок-конфига

	workersDefault           = 30
	workersMaxDefault        = 100
	wokersMaxPerQueueDefault = workersMaxDefault
	workersMaxLimit          = 5
)

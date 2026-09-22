//go:build integration || stress

package tptest

import "time"

const (
	testConfPrefix        = "/"
	testInstanceName      = "test"
	testRawQueueID        = 65534
	testAddonsQueueID     = 65533
	testNoHandlerQueueID  = 65532
	testSleeperQueueID    = 65531
	testShardingQueueID   = 65530
	testRetryEventQueueID = 65529
	testUnusedQueueID     = 65528
	testBatchSize         = 10

	// Адреса шардов queued в compose-сети dev-env: интеграционные тесты выполняются в сервисе
	// tests (см. dev-env/docker-compose.yaml и make test-integration).
	testShard0Addr = "queued_shard0:20000"
	testShard1Addr = "queued_shard1:20000"

	// Сетевые алиасы тех же контейнеров (networks.default.aliases в docker-compose.yaml):
	// go-iprotoconn дедуплицирует соединения по строке адреса process-wide, поэтому тесту,
	// которому нужно собственное соединение или "переезд" шарда на новый адрес, нужен
	// другой адрес того же сервера.
	testShard0ProbeAddr = "queued-shard0-probe:20000" // TestCancelDoesNotKillConnection
	testShard0MovedAddr = "queued-shard0-moved:20000" // TestHotReconnect
	testShard1MovedAddr = "queued-shard1-moved:20000" // TestHotReconnect
	testDefer           = 5 * time.Second
	testPollDelay       = time.Second

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

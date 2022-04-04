//go:build integration
// +build integration

package tptest

import "time"

const (
	testConfPrefix            = "/project/queued"
	testInstanceName          = "test"
	testRawQueueID            = 65534
	testQueueWithAddonsID     = 65533
	testQueueWithoutHandlerID = 65532
	testQueueSleeperID        = 65531
	testBatchSize             = 10
	testDefer                 = 2 * time.Second
	testPollDelay             = time.Second
)

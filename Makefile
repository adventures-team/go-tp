GO = go
GOSRC = $(shell find . -type f -name '*.go')
TEST_TIMEOUT = 10s
LINT_TIMEOUT = 5m
GOFLAGS =
# Образ Go для интеграционных тестов (сервис tests в dev-env/docker-compose.yaml): версия go из go.mod.
GOIMAGE = golang:1.26.3-trixie

.PHONY: all
all: format deps test full-lint doc queued-stresser

.PHONY: queued-stresser
queued-stresser: bin/queued-stresser

bin/queued-stresser: cmd/queued-stresser/*.go go.mod go.sum
	$(GO) build -o $@ ./cmd/queued-stresser

.PHONY: help
help:
	@echo "\nTARGETS:\n"
	@sed -En 's/^([a-z][a-z0-9/._-]*):.*/\1/pi' Makefile

.PHONY: deps
deps:
	$(GO) mod tidy -v && $(GO) mod verify

.PHONY: lint
lint:
	$(info #Running lint...)
	golangci-lint run --timeout=$(LINT_TIMEOUT) --build-tags=golangci --new-from-rev=origin/$(shell (grep -s DEFAULT_BRANCH: .gitlab-ci.yml | cut -d: -f2 | sed 's/^\s*$$/master/' | tr -d ' \t'; echo master) | head -n1) ./...
	golangci-lint run --timeout=$(LINT_TIMEOUT) --build-tags=golangci,integration --new-from-rev=origin/$(shell (grep -s DEFAULT_BRANCH: .gitlab-ci.yml | cut -d: -f2 | sed 's/^\s*$$/master/' | tr -d ' \t'; echo master) | head -n1) ./...

.PHONY: full-lint
full-lint:
	$(info #Running full lint...)
	golangci-lint run --build-tags=golangci ./...
	golangci-lint run --build-tags=golangci,integration ./...

.PHONY: format
format:
	find . -name '*.go' | grep -v /vendor/ | xargs $(GO) tool goimports -local gitlab.adv.team/ -l -w

.PHONY: test
test:
	$(GO) test $(GOFLAGS) -v     \
	    -timeout $(TEST_TIMEOUT) \
	    -race                    \
	    -covermode atomic        \
	    -coverprofile cover.out  \
	    -coverpkg ./...          \
	    ./...
	$(GO) tool cover -html cover.out -o cover.html

# Интеграционные тесты выполняются внутри compose-сервиса tests (образ $(GOIMAGE)) - в одной
# сети с шардами queued, поэтому адреса шардов в тестах - имена и алиасы контейнеров, а не
# проброшенные на хост порты. В контейнер монтируются репозиторий и кеши модулей/сборки
# хоста (см. dev-env/docker-compose.yaml). $(GO) хоста нужен только для HTML-отчёта покрытия.
.PHONY: test-integration
test-integration: dev_env_start
	GOIMAGE=$(GOIMAGE) GOMODCACHE=$$($(GO) env GOMODCACHE) GOCACHE=$$($(GO) env GOCACHE) \
	TEST_UID=$$(id -u) TEST_GID=$$(id -g)                                              \
	docker compose -f dev-env/docker-compose.yaml run --rm tests                        \
	    go test $(GOFLAGS) -v                \
	    -tags=integration                   \
	    -race                               \
	    -covermode atomic                   \
	    -coverprofile cover.integration.out \
	    -coverpkg ./...                     \
	    ./test
	$(GO) tool cover -html cover.integration.out -o cover.integration.html

.PHONY: bench
bench:
	$(GO) test $(GOFLAGS) -v -bench . -run Benchmark ./...

.PHONY: generate
generate: generate-mocks generate-go generate-cdb

.PHONY: generate-go
generate-go:
	$(GO) generate ./...

.PHONY: generate-mocks
generate-mocks:
	$(GO) tool mockgen -destination test/mock/queue/mock_queue.go -typed -package mockqueue gitlab.adv.team/go/tp.git Queue
	$(GO) tool mockgen -destination test/mock/event/mock_retryable_event.go -typed -package mockevent gitlab.adv.team/go/tp.git RetryableEvent
	$(GO) tool mockgen -destination test/mock/slotpool/mock_slotpool.go -typed -package mockslotpool gitlab.adv.team/go/tp.git/managed/master Pool
	$(GO) tool mockgen -destination test/mock/stats/mock_stat.go -typed -package mockstat gitlab.adv.team/go/tp.git/managed/master SlotStat
	$(GO) tool mockgen -destination test/mock/poller/mock_poller.go -typed -package mockpoller gitlab.adv.team/go/tp.git/workerpool Poller,Worker,WorkerStat
	$(GO) tool mockgen -destination test/mock/onlineconf/mock_onlineconf.go -typed -package mockonlineconf gitlab.adv.team/go/tp.git/queueconfig OnlineConf
	$(GO) tool mockgen -destination test/mock/onlineconf/mock_onlineconf_subtree.go -typed -package mockonlineconf gitlab.adv.team/go/tp.git/test OnlineConfWithSubtree

.PHONY: generate-cdb
generate-cdb:
	$(GO) tool yaml2cdb -in test/examples/config.yaml -out test/examples/config.cdb
	$(GO) tool yaml2cdb -in dev-env/qm/conf/config.yaml -out dev-env/qm/conf/TREE.cdb

.PHONY: doc
doc: docs/DOCUMENTATION.md

docs/DOCUMENTATION.md: $(GOSRC)
	gomarkdoc --tags gomarkdoc --exclude-dirs ./test/... --output $@ ./...

# Запуск докеров с шардами queued для интеграционных тестов
.PHONY: dev_env_start
dev_env_start:
	docker compose -f dev-env/docker-compose.yaml up -d

# Остановка докеров с шардами queued для интеграционных тестов
.PHONY: dev_env_stop
dev_env_stop:
	docker compose -f dev-env/docker-compose.yaml rm -sf

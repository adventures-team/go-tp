GO = go
GOSRC = $(shell find . -type f -name '*.go')
TEST_TIMEOUT = 10s
LINT_TIMEOUT = 5m

# Устанавливаем для gotip язык явно, чтобы для экспериментов с дженериками не приходилось править язык в go.mod.
# 2022-06-08: перешли на go1.18, однако у коллег локально могут быть устаревшие версии, поэтому пока юзаем этот костыль.
# При обновлении версии в go.mod этот блок нужно выпилить.
GOFLAGS=-gcflags=all=-lang=$(shell go version | cut -d\  -f3 | cut -d. -f1,2)

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

# Устанавливает зависимости проекта
.PHONY: deps
deps:
	$(GO) mod tidy -v && $(GO) mod verify

# Линт на отличия от мастера
.PHONY: lint
lint:
	$(info #Running lint...)
	golangci-lint run --timeout=$(LINT_TIMEOUT) --build-tags=golangci --new-from-rev=origin/$(shell (grep -s DEFAULT_BRANCH: .gitlab-ci.yml | cut -d: -f2 | sed 's/^\s*$$/master/' | tr -d ' \t'; echo master) | head -n1) ./...
	golangci-lint run --timeout=$(LINT_TIMEOUT) --build-tags=golangci,integration --new-from-rev=origin/$(shell (grep -s DEFAULT_BRANCH: .gitlab-ci.yml | cut -d: -f2 | sed 's/^\s*$$/master/' | tr -d ' \t'; echo master) | head -n1) ./...

# Линт на весь код
.PHONY: full-lint
full-lint:
	$(info #Running full lint...)
	golangci-lint run --build-tags=golangci ./...
	golangci-lint run --build-tags=golangci,integration ./...

# Форматирует код с помощью goimports
.PHONY: format
format:
	find . -name '*.go' | grep -v /vendor/ | xargs goimports -local gitlab.corp.my.games/ -l -w

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

.PHONY: test-integration
test-integration: dev_env_start
	$(GO) test $(GOFLAGS) -v                \
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
generate:
	$(GO) generate ./...

.PHONY: generate_mocks
generate_mocks:
	mockgen -destination test/mock/queue/mock_queue.go -typed -package mock_queue gitlab.corp.my.games/adventures/go/tp.git/managed/master Queue
	mockgen -destination test/mock/slotpool/mock_slotpool.go -typed -package mock_slotpool gitlab.corp.my.games/adventures/go/tp.git/managed/master Pool
	mockgen -destination test/mock/stats/mock_stat.go -typed -package mock_stat gitlab.corp.my.games/adventures/go/tp.git/managed/master SlotStat
	mockgen -destination test/mock/poller/mock_poller.go -typed -package mock_poller gitlab.corp.my.games/adventures/go/tp.git/workerpool Poller,Worker,WorkerStat
	mockgen -destination test/mock/iproto/mock_iproto.go -typed -package mock_iproto gitlab.corp.my.games/adventures/go/tp.git/managed/qmclient Connection

.PHONY: doc
doc: API.md # doc/index.html

API.md: $(GOSRC)
	gomarkdoc --tags gomarkdoc --output $@ ./...

doc/index.html: $(GOSRC)
	godoc-static -destination=doc .

# Запуск докеров с шардами queued для интеграционных тестов
.PHONY: dev_env_start
dev_env_start:
	docker compose -f dev-env/docker-compose.yaml up -d

# Остановка докеров с шардами queued для интеграционных тестов
.PHONY: dev_env_stop
dev_env_stop:
	docker compose -f dev-env/docker-compose.yaml rm -sf

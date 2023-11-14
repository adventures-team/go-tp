GO = go
GOSRC = $(shell find . -type f -name '*.go')
TEST_TIMEOUT = 10s

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
	golangci-lint run --build-tags=golangci --new-from-rev=origin/$(shell (grep -s DEFAULT_BRANCH: .gitlab-ci.yml | cut -d: -f2 | sed 's/^\s*$$/master/' | tr -d ' \t'; echo master) | head -n1) ./...
	golangci-lint run --build-tags=golangci,integration --new-from-rev=origin/$(shell (grep -s DEFAULT_BRANCH: .gitlab-ci.yml | cut -d: -f2 | sed 's/^\s*$$/master/' | tr -d ' \t'; echo master) | head -n1) ./...

# Линт на весь код
.PHONY: full-lint
full-lint:
	$(info #Running full lint...)
	golangci-lint run --build-tags=golangci ./...
	golangci-lint run --build-tags=golangci,integration ./...

# Форматирует код с помощью goimports
.PHONY: format
format:
	find . -name '*.go' | grep -v /vendor/ | xargs goimports -local gitlab.corp.mail.ru/ -l -w

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
	    ./tests
	$(GO) tool cover -html cover.integration.out -o cover.integration.html

.PHONY: bench
bench:
	$(GO) test $(GOFLAGS) -v -bench . -run Benchmark ./...

.PHONY: generate
generate:
	$(GO) generate ./...

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

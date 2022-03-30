TEST_TIMEOUT=10s

GO = go
GOSRC = $(shell find . -type f -name '*.go')

# Устанавливаем для gotip язык явно, чтобы для экспериментов с дженериками не приходилось править язык в go.mod
ifeq ($(GO),go)
GOFLAGS=
else
GOFLAGS=-gcflags=-lang=go1.18
endif

.PHONY: all
all:
	$(info TODO)

.PHONY: help
help:
	@echo "\nTARGETS:\n"
	@$(MAKE) -qpRr | egrep -e '^[a-z].*:$$' | sed -e 's~:~~g' | sort | uniq
	@echo ""

# Устанавливает зависимости проекта
.PHONY: deps
deps:
	$(GO) mod tidy -v && $(GO) mod verify

# Линт на отличия от мастера
.PHONY: lint
lint:
	$(info #Running lint...)
	golangci-lint run --new-from-rev=origin/$(shell (grep -s DEFAULT_BRANCH: .gitlab-ci.yml | cut -d: -f2 | sed 's/^\s*$$/master/' | tr -d ' \t'; echo master) | head -n1) ./...

# Линт на весь код
.PHONY: full-lint
full-lint:
	$(info #Running full lint...)
	golangci-lint run ./...

# Форматирует код с помощью goimports
.PHONY: format
format:
	find . -name '*.go' | grep -v /vendor/ | xargs goimports -local gitlab.corp.mail.ru/ -l -w

.PHONY: test
test:
	$(GO) test $(GOFLAGS) -v     \
	    -timeout $(TEST_TIMEOUT) \
	    -covermode count         \
	    -coverprofile cover.out  \
	    -coverpkg ./...          \
	    ./...
	$(GO) tool cover -html cover.out -o cover.html

.PHONY: test-integration
test-integration:
	$(GO) test $(GOFLAGS) -v                \
	    -tags=integration                   \
	    -covermode count                    \
	    -coverprofile cover.integration.out \
	    -coverpkg ./...                     \
	    ./...
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

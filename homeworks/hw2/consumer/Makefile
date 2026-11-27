SHELL := /bin/sh

GO ?= go
BINARY ?= bin/consumer
VERIFY_BINARY ?= bin/verify
CMD ?= ./cmd/consumer
VERIFY_CMD ?= ./cmd/verify
GOFLAGS ?=
BUILD_FLAGS ?= -trimpath
GO_LDFLAGS ?= -s -w
GOENV ?=
GOLANGCI_BIN ?= golangci-lint
GOLANGCI_LINT_VERSION ?= v1.64.8

.PHONY: all help build build-verify build-all test race vet lint lint-fix lint-install check fmt tidy run run-verify docker-build clean

all: check build-all ## Проверить код и собрать consumer и verify

help: ## Показать доступные цели
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_.-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: $(BINARY) ## Собрать consumer

$(BINARY): $(shell find . -type f -name '*.go') go.mod $(wildcard go.sum)
	@mkdir -p "$(@D)"
	$(GO) build $(GOFLAGS) $(BUILD_FLAGS) -ldflags "$(GO_LDFLAGS)" -o "$@" $(CMD)

build-verify: $(VERIFY_BINARY) ## Собрать CLI verify

$(VERIFY_BINARY): $(shell find . -type f -name '*.go') go.mod $(wildcard go.sum)
	@mkdir -p "$(@D)"
	$(GO) build $(GOFLAGS) $(BUILD_FLAGS) -ldflags "$(GO_LDFLAGS)" -o "$@" $(VERIFY_CMD)

build-all: build build-verify ## Собрать consumer и verify

test: ## Запустить unit-тесты
	$(GO) test $(GOFLAGS) -count=1 ./...

race: ## Запустить тесты с race detector
	$(GO) test $(GOFLAGS) -race -count=1 ./...

vet: ## Запустить go vet
	$(GO) vet $(GOFLAGS) ./...

lint: ## Запустить golangci-lint без изменения файлов
	$(GOENV) $(GOLANGCI_BIN) run ./... --timeout 2m

lint-fix: ## Запустить golangci-lint с автоматическим исправлением
	$(GOENV) $(GOLANGCI_BIN) run ./... --fix --timeout 2m

lint-install: ## Установить зафиксированную версию golangci-lint
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

check: lint vet test ## Запустить проверки без изменения файлов

fmt: lint-fix ## Отформатировать код и применить автоисправления линтера

tidy: ## Синхронизировать зависимости модуля
	$(GO) mod tidy

run: ## Запустить consumer без сборки бинарника
	$(GO) run $(GOFLAGS) $(CMD)

run-verify: ## Запустить verify без сборки бинарника
	$(GO) run $(GOFLAGS) $(VERIFY_CMD)

docker-build: ## Собрать Docker-образ приложения
	docker build -t highload-hw2-consumer:local .

clean: ## Удалить артефакты сборки
	rm -rf bin

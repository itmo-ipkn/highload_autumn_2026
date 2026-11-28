SHELL := /bin/sh

GO ?= go
BINARY ?= bin/producer
CMD ?= ./cmd/producer
GOFLAGS ?=
BUILD_FLAGS ?= -trimpath
GO_LDFLAGS ?= -s -w
GOENV ?=
GOLANGCI_BIN ?= golangci-lint
GOLANGCI_LINT_VERSION ?= v1.64.8

.PHONY: all help build test race vet lint lint-fix lint-install check fmt tidy run clean docker-build

all: check build ## Проверить код и собрать приложение

help: ## Показать доступные цели
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_.-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: $(BINARY) ## Собрать приложение

$(BINARY): $(shell find . -type f -name '*.go') go.mod $(wildcard go.sum)
	@mkdir -p "$(@D)"
	$(GO) build $(GOFLAGS) $(BUILD_FLAGS) -ldflags "$(GO_LDFLAGS)" -o "$@" $(CMD)

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

run: ## Запустить приложение без сборки бинарника
	$(GO) run $(GOFLAGS) $(CMD)

docker-build: ## Собрать Docker-образ приложения
	docker build -t highload-hw2-producer:local .

clean: ## Удалить артефакты сборки
	rm -rf bin

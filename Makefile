# flink-clj Makefile
#
# Common build and development tasks for the flink-clj project.
#
# Usage:
#   make help     - Show available targets
#   make setup    - Install dependencies and compile
#   make test     - Run all tests
#   make build    - Build release artifacts

.PHONY: help setup deps compile test test-1.20 test-2.x clean repl \
        build jar-1.20 jar-2.x uberjar release install check lint

# Default target
help:
	@echo "flink-clj Build System"
	@echo ""
	@echo "Setup & Development:"
	@echo "  make setup       - Install deps and compile (first time setup)"
	@echo "  make deps        - Download dependencies only"
	@echo "  make compile     - Compile Java and Clojure sources"
	@echo "  make repl        - Start development REPL (Flink 1.20)"
	@echo "  make repl-2.x    - Start development REPL (Flink 2.x)"
	@echo "  make clean       - Remove build artifacts"
	@echo ""
	@echo "Testing:"
	@echo "  make test        - Run tests for both Flink versions"
	@echo "  make test-1.20   - Run tests for Flink 1.20 only"
	@echo "  make test-2.x    - Run tests for Flink 2.x only"
	@echo "  make check       - Run tests and lint"
	@echo ""
	@echo "Building:"
	@echo "  make build       - Build JARs for both versions"
	@echo "  make jar-1.20    - Build library JAR for Flink 1.20"
	@echo "  make jar-2.x     - Build library JAR for Flink 2.x"
	@echo "  make uberjar     - Build CLI uberjar"
	@echo ""
	@echo "Release:"
	@echo "  make release     - Build all release artifacts"
	@echo "  make install     - Install CLI locally"
	@echo ""
	@echo "Examples:"
	@echo "  make run-example EXAMPLE=word-count"
	@echo ""

# ============================================================================
# Setup & Development
# ============================================================================

setup: deps compile
	@echo ""
	@echo "Setup complete! Try:"
	@echo "  make repl        - Start REPL"
	@echo "  make test        - Run tests"

deps:
	@echo "Downloading dependencies..."
	lein with-profile +flink-1.20,+dev deps
	lein with-profile +flink-2.x,+dev deps
	@echo "Dependencies downloaded."

compile: compile-1.20 compile-2.x
	@echo "Compilation complete."

compile-1.20:
	@echo "Compiling for Flink 1.20..."
	lein with-profile +flink-1.20 compile

compile-2.x:
	@echo "Compiling for Flink 2.x..."
	lein with-profile +flink-2.x compile

repl:
	lein with-profile +dev,+flink-1.20 repl

repl-2.x:
	lein with-profile +dev,+flink-2.x repl

clean:
	lein clean
	rm -rf target/
	rm -rf release/
	@echo "Cleaned."

# ============================================================================
# Testing
# ============================================================================

test: test-1.20 test-2.x
	@echo ""
	@echo "All tests passed!"

test-1.20:
	@echo "Testing with Flink 1.20..."
	lein with-profile +flink-1.20,+dev test

test-2.x:
	@echo "Testing with Flink 2.x..."
	lein with-profile +flink-2.x,+dev test

check: lint test
	@echo "All checks passed!"

lint:
	@echo "Running lint checks..."
	@if command -v clj-kondo > /dev/null; then \
		clj-kondo --lint src/clojure test; \
	else \
		echo "clj-kondo not installed, skipping lint"; \
	fi

# ============================================================================
# Building
# ============================================================================

build: jar-1.20 jar-2.x
	@echo ""
	@echo "Build complete. JARs in target/"

jar-1.20:
	@echo "Building JAR for Flink 1.20..."
	lein with-profile +flink-1.20 jar

jar-2.x:
	@echo "Building JAR for Flink 2.x..."
	lein with-profile +flink-2.x jar

uberjar:
	@echo "Building CLI uberjar..."
	lein with-profile +flink-1.20,+uberjar uberjar

# ============================================================================
# Release
# ============================================================================

RELEASE_DIR := release
VERSION := $(shell grep 'defproject' project.clj | sed 's/.*"\([^"]*\)".*/\1/')

release: clean test build-release
	@echo ""
	@echo "Release artifacts in $(RELEASE_DIR)/"
	@ls -la $(RELEASE_DIR)/

build-release: create-release-dir
	@echo "Building release artifacts for version $(VERSION)..."
	# Build library JARs
	lein with-profile +flink-1.20 jar
	cp target/flink-clj-$(VERSION).jar $(RELEASE_DIR)/flink-clj-1.20-$(VERSION).jar
	lein clean
	lein with-profile +flink-2.x jar
	cp target/flink-clj-$(VERSION).jar $(RELEASE_DIR)/flink-clj-2.x-$(VERSION).jar
	lein clean
	# Build CLI uberjar
	lein with-profile +flink-1.20,+uberjar uberjar
	cp target/flink-clj-$(VERSION)-standalone.jar $(RELEASE_DIR)/flink-clj-cli.jar
	# Copy scripts
	cp bin/flink-clj $(RELEASE_DIR)/
	cp install.sh $(RELEASE_DIR)/
	@echo "Release build complete."

create-release-dir:
	mkdir -p $(RELEASE_DIR)

install: uberjar
	@echo "Installing flink-clj CLI..."
	mkdir -p ~/.flink-clj
	cp target/flink-clj-*-standalone.jar ~/.flink-clj/flink-clj-cli.jar
	@if [ -w /usr/local/bin ]; then \
		cp bin/flink-clj /usr/local/bin/; \
	else \
		sudo cp bin/flink-clj /usr/local/bin/; \
	fi
	@echo "Installed! Run 'flink-clj help' to get started."

# ============================================================================
# Examples
# ============================================================================

EXAMPLE ?= word-count

run-example:
	@echo "Running example: $(EXAMPLE)"
	./run-example.sh $(EXAMPLE)

# ============================================================================
# Docker
# ============================================================================

docker-build:
	docker build -t flink-clj:latest .

docker-test:
	docker compose run --rm test

# ============================================================================
# CI Helpers
# ============================================================================

ci-setup:
	@echo "CI Setup..."
	lein version
	java -version

ci-test: ci-setup test
	@echo "CI tests complete."

ci-release: ci-setup release
	@echo "CI release complete."

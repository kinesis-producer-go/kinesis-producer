# Repository Guidelines

## Project Structure & Module Organization
This repository is a Go library for producing Amazon Kinesis records (`module github.com/kinesis-producer-go/kinesis-producer`).
- Core implementation files live at the repository root: `producer.go`, `aggregator.go`, `config.go`, `randkey.go`.
- Protocol buffer schema and generated code: `messages.proto` and `messages.pb.go`.
- Tests are colocated with source as `*_test.go` (for example, `producer_test.go`, `config_test.go`, `example_test.go`).
- Supporting docs are in root markdown files such as `README.md` and `aggregation-format.md`.
- CI configuration is in `.github/workflows/ci.yml`.

## Build, Test, and Development Commands
- `go mod download`: fetch dependencies.
- `go test -v ./...`: run the full test suite (same baseline as CI).
- `go test -race -v ./...`: run tests with the race detector (also enforced in CI).
- `make run-example`: run the example test flow (`TestExample`).
- `go test -v -run TestExample`: run only the example test directly.

If you update `messages.proto`, regenerate protobuf code with:
`protoc --go_out=. --go_opt=paths=source_relative messages.proto`

## Coding Style & Naming Conventions
- Follow standard Go style and keep code `gofmt`-formatted (`gofmt -w .` before submitting).
- Use tabs/formatting produced by `gofmt`; avoid manual alignment.
- Keep package-level API names clear and idiomatic (`New`, `Config`, `Put`, `Start`, `Stop`).
- File names should be lowercase with underscores only when needed (for example, `message_test.go`).

## Testing Guidelines
- Use Go’s `testing` package; `testify` is available for assertions.
- Place tests in `*_test.go` files next to the code they validate.
- Prefer table-driven tests for config validation and edge cases.
- Cover failure paths (AWS/Kinesis errors, buffering limits, shutdown behavior), not only happy paths.

## Commit & Pull Request Guidelines
- Use concise, imperative commit subjects with prefixes seen in history: `fix:`, `test:`, `deps:`, `docs:`, `refactor:`, `ci:`.
- All commits must be signed and signed-off: use `git commit -S -s -m "fix: ..."` (required).
- Keep commits focused; separate dependency bumps from behavior changes.
- PRs should include: purpose, behavior impact, test evidence (`go test -v ./...` output), and linked issue(s) when applicable.
- Update `README.md` or protocol docs when public behavior or message format changes.

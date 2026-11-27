package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"highload/hw2/consumer/internal/config"
	"highload/hw2/consumer/internal/verify"
)

const (
	exitFailure   = 1
	exitOrderOnly = 2
)

func main() { os.Exit(run(os.Stdout, log.Default(), verify.Run)) }

func run(out io.Writer, logger *log.Logger, check func(context.Context, config.VerifyConfig, *log.Logger) (verify.Result, error)) int {
	cfg, err := config.LoadVerify()
	login := strings.TrimSpace(cfg.GitHubLogin)
	if err != nil && login == "" {
		// LoadVerify may return a zero config; keep the CLI banner even on startup errors.
		login = strings.TrimSpace(os.Getenv("GITHUB_LOGIN"))
	}
	if login == "" {
		login = "ekubyshin"
	}
	fmt.Fprintf(out, "%s %s\n", login, time.Now().Format(time.RFC3339))
	if err != nil {
		logger.Printf("load verify config: %v", err)
		return exitFailure
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	result, err := check(ctx, cfg, logger)
	if err != nil {
		logger.Printf("verify failed: %v", err)
		return exitFailure
	}
	if result.Passed() {
		fmt.Fprintln(out, "PASS: no missing sequences; all poison events are in DLT; no ordering violations")
		return 0
	}
	fmt.Fprintf(out, "FAIL: missing sequences=%d, missing sink events=%d, sequence conflicts=%d, poison not in DLT=%d, non-poison in DLT=%d, order violations=%d, lag=%d\n",
		len(result.MissingSequences), len(result.MissingSinkEvents), len(result.SequenceConflicts), len(result.PoisonNotInDLT), len(result.NonPoisonInDLT), result.OrderViolations, result.ConsumerLag)
	printExamples(out, "missing", result.MissingSequences)
	printExamples(out, "missing_sink_event", result.MissingSinkEvents)
	printExamples(out, "sequence_conflict", result.SequenceConflicts)
	printExamples(out, "poison_not_in_dlt", result.PoisonNotInDLT)
	printExamples(out, "non_poison_in_dlt", result.NonPoisonInDLT)
	if result.OrderOnly() {
		return exitOrderOnly
	}
	return exitFailure
}

func printExamples(out io.Writer, name string, values []string) {
	const limit = 10
	for i, value := range values {
		if i == limit {
			fmt.Fprintf(out, "%s: ... and %d more\n", name, len(values)-limit)
			return
		}
		fmt.Fprintf(out, "%s: %s\n", name, value)
	}
}

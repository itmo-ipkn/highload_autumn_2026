package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
)

func githubLogin() (string, error) {
	if login := strings.TrimSpace(os.Getenv("GITHUB_LOGIN")); login != "" {
		return login, nil
	}
	data, err := output("git", "config", "--get", "remote.origin.url")
	if err == nil {
		match := regexp.MustCompile(`(?i)github\.com[:/]([^/]+)/`).FindSubmatch(data)
		if len(match) == 2 {
			return string(match[1]), nil
		}
	}
	return "", fmt.Errorf("set GITHUB_LOGIN or configure a GitHub origin")
}

func run(c compose) (int, error) {
	login, err := githubLogin()
	if err != nil {
		return 2, err
	}
	fmt.Printf("%s %s\n", login, time.Now().Format(time.RFC3339))
	before, beforeErr := c.progress()
	s := snapshot{users: make(map[int64]*userEvents), sink: make(map[string]struct{}), dlt: make(map[string]struct{})}
	if err := s.readSink(c); err != nil {
		return 2, fmt.Errorf("sink: %w", err)
	}
	if err := s.readDLT(c); err != nil {
		return 2, fmt.Errorf("DLT: %w", err)
	}
	after, afterErr := c.progress()
	if beforeErr != nil {
		afterErr = beforeErr
	}
	return s.report(before, after, afterErr), nil
}

func main() {
	var c compose
	flag.StringVar(&c.project, "p", os.Getenv("COMPOSE_PROJECT_NAME"), "Compose project name")
	flag.StringVar(&c.project, "project-name", os.Getenv("COMPOSE_PROJECT_NAME"), "Compose project name")
	flag.StringVar(&c.file, "f", "docker-compose.yml", "Compose file path")
	flag.Parse()
	code, err := run(c)
	if err != nil {
		fmt.Fprintf(os.Stderr, "verify: %v\n", err)
	}
	os.Exit(code)
}

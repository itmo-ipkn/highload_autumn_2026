package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os/exec"
)

type compose struct {
	file, project string
}

func (c compose) exec(service string, args ...string) ([]byte, error) {
	command := []string{"compose", "-f", c.file}
	if c.project != "" {
		command = append(command, "-p", c.project)
	}

	command = append(command, "exec", "-T", service)
	return output("docker", append(command, args...)...)
}

func (c compose) kafka(tool string, args ...string) ([]byte, error) {
	base := []string{"/opt/kafka/bin/" + tool + ".sh", "--bootstrap-server", "kafka-1:19092,kafka-2:19092,kafka-3:19092"}
	return c.exec("kafka-1", append(base, args...)...)
}

func output(program string, args ...string) ([]byte, error) {
	b, err := exec.Command(program, args...).Output()
	if e, ok := err.(*exec.ExitError); ok {
		return nil, fmt.Errorf("%s: %w: %s", program, e, bytes.TrimSpace(e.Stderr))
	}
	return b, err
}

func lines(data []byte, each func(string) error) error {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), 128<<20)

	for scanner.Scan() {
		if err := each(scanner.Text()); err != nil {
			return err
		}
	}
	return scanner.Err()
}

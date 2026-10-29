package credentials

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	usernameFile    = "username"
	passwordFile    = "password"
	defaultUsername = "admin"
	passwordBytes   = 24
	maxSecretBytes  = 1024
	grafanaDBName   = "grafana.db"
)

var (
	ErrConflict      = errors.New("saved credentials do not match the explicit environment")
	ErrIncomplete    = errors.New("saved credentials are incomplete")
	ErrGrafanaOrphan = errors.New("Grafana database exists without saved credentials")
	ErrInvalidSecret = errors.New("credentials are invalid")
)

type Options struct {
	Dir            string
	GrafanaDataDir string
	Username       string
	Password       string
	GroupID        int
}

type Status struct {
	Created bool
}

type savedState int

const (
	savedNone savedState = iota
	savedIncomplete
	savedReady
)

func Ensure(opts Options) (Status, error) {
	if opts.Dir == "" {
		return Status{}, errors.New("credentials directory is required")
	}
	if opts.GrafanaDataDir == "" {
		return Status{}, errors.New("Grafana data directory is required")
	}
	if opts.GroupID <= 0 {
		return Status{}, errors.New("credentials group id must be positive")
	}
	username := opts.Username
	if username != "" {
		if err := validateUsername(username); err != nil {
			return Status{}, err
		}
	}
	if opts.Password != "" {
		if err := validatePassword(opts.Password); err != nil {
			return Status{}, err
		}
	}

	if err := prepareDir(opts.Dir, opts.GroupID); err != nil {
		return Status{}, err
	}

	savedUser, savedPass, state, err := readSaved(opts.Dir)
	if err != nil {
		return Status{}, err
	}
	dbExists, err := grafanaDBExists(opts.GrafanaDataDir)
	if err != nil {
		return Status{}, err
	}

	if state == savedReady {
		if username != "" && !secretEqual(username, savedUser) {
			return Status{}, fmt.Errorf("%w: GRAFANA_ADMIN_USER differs from the saved username; empty the variable to keep the saved pair, or reset the credentials and Grafana volumes together with \"docker compose down -v\"", ErrConflict)
		}
		if opts.Password != "" && !secretEqual(opts.Password, savedPass) {
			return Status{}, fmt.Errorf("%w: GRAFANA_ADMIN_PASSWORD differs from the saved password; an empty password keeps the saved pair, and the initializer does not rotate it. Reset both volumes with \"docker compose down -v\" to choose another password", ErrConflict)
		}
		if err := writePair(opts.Dir, savedUser, savedPass, opts.GroupID); err != nil {
			return Status{}, err
		}
		return Status{Created: false}, nil
	}

	if dbExists {
		if state == savedIncomplete {
			return Status{}, fmt.Errorf("%w: restore both username and password files together with the Grafana data volume, or reset both volumes with \"docker compose down -v\"", ErrIncomplete)
		}
		return Status{}, fmt.Errorf("%w: restore the credentials volume together with the Grafana data volume, or reset both with \"docker compose down -v\" (this deletes the saved dashboard data)", ErrGrafanaOrphan)
	}

	if username == "" {
		username = defaultUsername
	}
	password := opts.Password
	if password == "" {
		password, err = generatePassword()
		if err != nil {
			return Status{}, err
		}
	}
	if err := writePair(opts.Dir, username, password, opts.GroupID); err != nil {
		return Status{}, err
	}
	return Status{Created: true}, nil
}

func prepareDir(dir string, gid int) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create credentials directory: %w", err)
	}
	if err := os.Chmod(dir, 0o750); err != nil {
		return fmt.Errorf("chmod credentials directory: %w", err)
	}
	return chownIfRoot(dir, gid)
}

func readSaved(dir string) (string, string, savedState, error) {
	userPath := filepath.Join(dir, usernameFile)
	passPath := filepath.Join(dir, passwordFile)
	_, userErr := os.Lstat(userPath)
	_, passErr := os.Lstat(passPath)
	userExists := userErr == nil
	passExists := passErr == nil
	if userErr != nil && !errors.Is(userErr, os.ErrNotExist) {
		return "", "", savedNone, fmt.Errorf("check saved username: %w", userErr)
	}
	if passErr != nil && !errors.Is(passErr, os.ErrNotExist) {
		return "", "", savedNone, fmt.Errorf("check saved password: %w", passErr)
	}
	if userExists != passExists {
		return "", "", savedIncomplete, nil
	}
	if !userExists {
		return "", "", savedNone, nil
	}
	username, err := readSecret(userPath)
	if err != nil {
		return "", "", savedIncomplete, nil
	}
	password, err := readSecret(passPath)
	if err != nil {
		return "", "", savedIncomplete, nil
	}
	if err := validateUsername(username); err != nil || validatePassword(password) != nil {
		return "", "", savedIncomplete, nil
	}
	return username, password, savedReady, nil
}

func readSecret(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("%w: symlink is not allowed", ErrInvalidSecret)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("%w: file is not regular", ErrInvalidSecret)
	}
	if info.Size() > maxSecretBytes {
		return "", fmt.Errorf("%w: file is too large", ErrInvalidSecret)
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxSecretBytes+1))
	if err != nil {
		return "", err
	}
	if len(data) > maxSecretBytes {
		return "", fmt.Errorf("%w: file is too large", ErrInvalidSecret)
	}
	return string(data), nil
}

func grafanaDBExists(dataDir string) (bool, error) {
	path := filepath.Join(dataDir, grafanaDBName)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check Grafana database: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return false, fmt.Errorf("Grafana database path is not a regular file")
	}
	return true, nil
}

func writePair(dir, username, password string, gid int) error {
	if err := writeSecret(dir, usernameFile, username, gid); err != nil {
		return err
	}
	if err := writeSecret(dir, passwordFile, password, gid); err != nil {
		return err
	}
	return syncDir(dir)
}

func writeSecret(dir, name, value string, gid int) error {
	finalPath := filepath.Join(dir, name)
	tmp, err := os.CreateTemp(dir, "."+name+"-")
	if err != nil {
		return fmt.Errorf("create credentials file: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	if err := tmp.Chmod(0o640); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod credentials file: %w", err)
	}
	if err := chownFileIfRoot(tmp, gid); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.WriteString(value); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write credentials file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync credentials file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close credentials file: %w", err)
	}
	if err := os.Rename(tmpName, finalPath); err != nil {
		return fmt.Errorf("replace credentials file: %w", err)
	}
	cleanup = false
	if err := os.Chmod(finalPath, 0o640); err != nil {
		return fmt.Errorf("chmod credentials file: %w", err)
	}
	return chownIfRoot(finalPath, gid)
}

func syncDir(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open credentials directory: %w", err)
	}
	defer handle.Close()
	if err := handle.Sync(); err != nil {
		return fmt.Errorf("sync credentials directory: %w", err)
	}
	return nil
}

func chownIfRoot(path string, gid int) error {
	if os.Geteuid() != 0 {
		return nil
	}
	if err := os.Chown(path, 0, gid); err != nil {
		return fmt.Errorf("chown %s: %w", filepath.Base(path), err)
	}
	return nil
}

func chownFileIfRoot(file *os.File, gid int) error {
	if os.Geteuid() != 0 {
		return nil
	}
	if err := file.Chown(0, gid); err != nil {
		return fmt.Errorf("chown credentials file: %w", err)
	}
	return nil
}

func generatePassword() (string, error) {
	buf := make([]byte, passwordBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate password: %w", err)
	}
	password := base64.RawURLEncoding.EncodeToString(buf)
	if err := validatePassword(password); err != nil {
		return "", err
	}
	return password, nil
}

func validateUsername(username string) error {
	if username == "" {
		return fmt.Errorf("%w: username is empty", ErrInvalidSecret)
	}
	if len(username) > 64 {
		return fmt.Errorf("%w: username is too long", ErrInvalidSecret)
	}
	for _, r := range username {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '.' || r == '_' || r == '-' || r == '@':
		default:
			return fmt.Errorf("%w: username contains an unsupported character", ErrInvalidSecret)
		}
	}
	return nil
}

func validatePassword(password string) error {
	if password == "" {
		return fmt.Errorf("%w: password is empty", ErrInvalidSecret)
	}
	if len(password) > 128 {
		return fmt.Errorf("%w: password is too long", ErrInvalidSecret)
	}
	if strings.ContainsAny(password, "\x00\r\n") {
		return fmt.Errorf("%w: password contains a forbidden control character", ErrInvalidSecret)
	}
	return nil
}

func secretEqual(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

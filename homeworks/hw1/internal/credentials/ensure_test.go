package credentials

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestEnsureGeneratesAndReusesPassword(t *testing.T) {
	dir := t.TempDir()
	data := t.TempDir()
	opts := Options{Dir: dir, GrafanaDataDir: data, Username: "admin", GroupID: 472}

	first, err := Ensure(opts)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !first.Created {
		t.Fatal("expected created credentials")
	}
	saved := readPair(t, dir)
	if saved.user != "admin" {
		t.Fatalf("username = %s", saved.user)
	}
	if len(saved.password) < 16 || strings.ContainsAny(saved.password, "\r\n") {
		t.Fatal("generated password has an unexpected shape")
	}
	assertPrivate(t, dir)

	second, err := Ensure(opts)
	if err != nil {
		t.Fatalf("reuse: %v", err)
	}
	if second.Created {
		t.Fatal("expected saved credentials to be reused")
	}
	again := readPair(t, dir)
	if again != saved {
		t.Fatal("reused credentials changed")
	}

	withoutPassword, err := Ensure(Options{Dir: dir, GrafanaDataDir: data, Username: "admin", GroupID: 472})
	if err != nil {
		t.Fatalf("reuse with empty password: %v", err)
	}
	if withoutPassword.Created {
		t.Fatal("empty password replaced saved credentials")
	}
}

func TestEnsureKeepsExplicitPassword(t *testing.T) {
	dir := t.TempDir()
	data := t.TempDir()
	const password = `pa$$(id)'" * ; ` + "`whoami`"
	opts := Options{Dir: dir, GrafanaDataDir: data, Username: "admin", Password: password, GroupID: 472}

	if _, err := Ensure(opts); err != nil {
		t.Fatalf("create: %v", err)
	}
	if got := readPair(t, dir); got.password != password {
		t.Fatal("explicit password was not stored verbatim")
	}
	if _, err := Ensure(opts); err != nil {
		t.Fatalf("reuse: %v", err)
	}
	if got := readPair(t, dir); got.password != password {
		t.Fatal("explicit password changed on reuse")
	}
}

func TestEnsureRejectsConflictingEnvironment(t *testing.T) {
	dir := t.TempDir()
	data := t.TempDir()
	const (
		user     = "admin"
		password = "stored-password"
		changed  = "other-password"
	)
	opts := Options{Dir: dir, GrafanaDataDir: data, Username: user, Password: password, GroupID: 472}
	if _, err := Ensure(opts); err != nil {
		t.Fatalf("create: %v", err)
	}
	before := readPair(t, dir)

	_, err := Ensure(Options{Dir: dir, GrafanaDataDir: data, Username: user, Password: changed, GroupID: 472})
	if err == nil || !strings.Contains(err.Error(), "does not rotate") {
		t.Fatalf("password conflict = %v", err)
	}
	if strings.Contains(err.Error(), password) || strings.Contains(err.Error(), changed) {
		t.Fatal("password conflict leaked a secret")
	}

	_, err = Ensure(Options{Dir: dir, GrafanaDataDir: data, Username: "other-admin", Password: password, GroupID: 472})
	if err == nil || !strings.Contains(err.Error(), "GRAFANA_ADMIN_USER") {
		t.Fatalf("username conflict = %v", err)
	}
	if strings.Contains(err.Error(), "other-admin") || strings.Contains(err.Error(), password) {
		t.Fatal("username conflict leaked a value")
	}
	if got := readPair(t, dir); got != before {
		t.Fatal("conflict replaced saved credentials")
	}
}

func TestEnsureRefusesDatabaseWithoutCredentials(t *testing.T) {
	dir := t.TempDir()
	data := t.TempDir()
	if err := os.WriteFile(filepath.Join(data, "grafana.db"), []byte("sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Ensure(Options{Dir: dir, GrafanaDataDir: data, Username: "admin", GroupID: 472})
	if err == nil || !strings.Contains(err.Error(), "docker compose down -v") {
		t.Fatalf("orphan database = %v", err)
	}
	if _, statErr := os.Lstat(filepath.Join(dir, passwordFile)); !os.IsNotExist(statErr) {
		t.Fatal("orphan database created a password")
	}
}

func TestEnsureReusesCredentialsWhenDatabaseExists(t *testing.T) {
	dir := t.TempDir()
	data := t.TempDir()
	opts := Options{Dir: dir, GrafanaDataDir: data, Username: "admin", Password: "stored-password", GroupID: 472}
	if _, err := Ensure(opts); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := os.WriteFile(filepath.Join(data, "grafana.db"), []byte("sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	status, err := Ensure(Options{Dir: dir, GrafanaDataDir: data, Username: "admin", GroupID: 472})
	if err != nil {
		t.Fatalf("reuse with database: %v", err)
	}
	if status.Created {
		t.Fatal("existing database caused a new password")
	}
}

func TestEnsureRepairsIncompleteCredentialsBeforeDatabaseExists(t *testing.T) {
	dir := t.TempDir()
	data := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, usernameFile), []byte("admin"), 0o640); err != nil {
		t.Fatal(err)
	}
	status, err := Ensure(Options{Dir: dir, GrafanaDataDir: data, Username: "admin", Password: "stored-password", GroupID: 472})
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	if !status.Created {
		t.Fatal("expected a new pair before Grafana has a database")
	}
	if got := readPair(t, dir); got.password != "stored-password" {
		t.Fatal("repaired password was not stored")
	}
}

func TestEnsureRejectsIncompleteCredentialsWhenDatabaseExists(t *testing.T) {
	dir := t.TempDir()
	data := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, usernameFile), []byte("admin"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(data, "grafana.db"), []byte("sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Ensure(Options{Dir: dir, GrafanaDataDir: data, Username: "admin", Password: "stored-password", GroupID: 472})
	if err == nil || !strings.Contains(err.Error(), "docker compose down -v") {
		t.Fatalf("incomplete with database = %v", err)
	}
	if strings.Contains(err.Error(), "stored-password") {
		t.Fatal("incomplete credentials leaked a secret")
	}
	if _, statErr := os.Lstat(filepath.Join(dir, passwordFile)); !os.IsNotExist(statErr) {
		t.Fatal("incomplete credentials created a password")
	}
}

func TestEnsureRejectsInvalidSecrets(t *testing.T) {
	dir := t.TempDir()
	data := t.TempDir()
	_, err := Ensure(Options{Dir: dir, GrafanaDataDir: data, Username: "bad user", Password: "stored-password", GroupID: 472})
	if err == nil {
		t.Fatal("accepted invalid username")
	}
	_, err = Ensure(Options{Dir: dir, GrafanaDataDir: data, Username: "admin", Password: "bad\npassword", GroupID: 472})
	if err == nil {
		t.Fatal("accepted password with newline")
	}
}

func TestEnsurePermissionsIgnoreUmask(t *testing.T) {
	old := syscall.Umask(0)
	t.Cleanup(func() { syscall.Umask(old) })

	dir := t.TempDir()
	data := t.TempDir()
	if _, err := Ensure(Options{Dir: dir, GrafanaDataDir: data, Username: "admin", Password: "stored-password", GroupID: 472}); err != nil {
		t.Fatalf("create: %v", err)
	}
	assertPrivate(t, dir)
}

func TestEnsureDefaultsUsername(t *testing.T) {
	dir := t.TempDir()
	data := t.TempDir()
	if _, err := Ensure(Options{Dir: dir, GrafanaDataDir: data, GroupID: 472}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if got := readPair(t, dir); got.user != "admin" {
		t.Fatalf("username = %s", got.user)
	}
}

type pair struct {
	user     string
	password string
}

func readPair(t *testing.T, dir string) pair {
	t.Helper()
	user, err := os.ReadFile(filepath.Join(dir, usernameFile))
	if err != nil {
		t.Fatal(err)
	}
	password, err := os.ReadFile(filepath.Join(dir, passwordFile))
	if err != nil {
		t.Fatal(err)
	}
	return pair{user: string(user), password: string(password)}
}

func assertPrivate(t *testing.T, dir string) {
	t.Helper()
	info, err := os.Lstat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o007 != 0 {
		t.Fatalf("directory mode = %o", info.Mode().Perm())
	}
	for _, name := range []string{usernameFile, passwordFile} {
		fileInfo, err := os.Lstat(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		perm := fileInfo.Mode().Perm()
		if perm != 0o640 {
			t.Fatalf("%s mode = %o", name, perm)
		}
	}
}

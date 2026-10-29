package credentials

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestWrapperExportsLiteralSecrets(t *testing.T) {
	script := wrapperScript(t)
	body, err := os.ReadFile(script)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, forbidden := range []string{"source ", "eval "} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("wrapper contains %q", forbidden)
		}
	}

	dir := t.TempDir()
	const (
		username = "admin"
		password = `pa$$(echo leaked)'" * ; ` + "`whoami`"
	)
	writeSecretFile(t, dir, usernameFile, username)
	writeSecretFile(t, dir, passwordFile, password)

	gotUser, gotPassword := runWrapper(t, dir, "generator", "GRAFANA_ADMIN_USER", "GRAFANA_ADMIN_PASSWORD")
	if gotUser != username || gotPassword != password {
		t.Fatal("generator export did not preserve the credential files")
	}
	if gotPassword == "leaked" || gotPassword == "whoami" {
		t.Fatal("wrapper executed credential contents")
	}

	gotUser, gotPassword = runWrapper(t, dir, "grafana", "GF_SECURITY_ADMIN_USER", "GF_SECURITY_ADMIN_PASSWORD")
	if gotUser != username || gotPassword != password {
		t.Fatal("grafana export did not preserve the credential files")
	}
}

func TestWrapperRejectsMissingFilesWithoutPrintingSecrets(t *testing.T) {
	dir := t.TempDir()
	const password = "not-stored"
	writeSecretFile(t, dir, passwordFile, password)

	cmd := exec.Command("/bin/sh", wrapperScript(t), "printf", "%s", "unreachable")
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"GRAFANA_CREDENTIALS_DIR=" + dir,
		"GRAFANA_CREDENTIALS_EXPORT=generator",
	}
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("wrapper succeeded without a username file")
	}
	if strings.Contains(string(out), password) {
		t.Fatal("wrapper printed a secret")
	}
	if !strings.Contains(string(out), "missing") {
		t.Fatalf("wrapper error = %s", out)
	}
}

func runWrapper(t *testing.T, dir, mode, userVar, passwordVar string) (string, string) {
	t.Helper()
	script := `printf '%s\n%s' "$` + userVar + `" "$` + passwordVar + `"`
	cmd := exec.Command("/bin/sh", wrapperScript(t), "/bin/sh", "-c", script)
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"GRAFANA_CREDENTIALS_DIR=" + dir,
		"GRAFANA_CREDENTIALS_EXPORT=" + mode,
	}
	out, err := cmd.Output()
	if err != nil {
		var exit *exec.ExitError
		if errorsAs(err, &exit) {
			t.Fatalf("wrapper failed: %s", exit.Stderr)
		}
		t.Fatal(err)
	}
	parts := strings.SplitN(string(out), "\n", 2)
	if len(parts) != 2 {
		t.Fatal("wrapper output did not contain both values")
	}
	return parts[0], parts[1]
}

func wrapperScript(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "deploy", "scripts", "with-grafana-credentials.sh")
}

func writeSecretFile(t *testing.T, dir, name, value string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o640); err != nil {
		t.Fatal(err)
	}
}

func errorsAs(err error, target **exec.ExitError) bool {
	exit, ok := err.(*exec.ExitError)
	if !ok {
		return false
	}
	*target = exit
	return true
}

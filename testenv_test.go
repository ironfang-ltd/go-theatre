package theatre

import (
	"bufio"
	"os"
	"strings"
	"testing"
)

// TestMain loads a .env file (if present) before running tests, so that
// THEATRE_TEST_DSN and other variables can be set without exporting them
// in the shell. Lines must be KEY=VALUE (no quotes are stripped, # comments
// and blank lines are skipped).
func TestMain(m *testing.M) {
	loadDotEnv(".env")
	os.Exit(m.Run())
}

func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return // file not found — not an error
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		// Don't overwrite existing env vars (explicit env takes precedence).
		if os.Getenv(key) == "" {
			os.Setenv(key, value)
		}
	}
}

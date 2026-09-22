package tptest

import (
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// repoRoot возвращает корень репозитория (родитель каталога test/).
func repoRoot() string {
	_, thisFile, _, _ := runtime.Caller(0)

	return filepath.Dir(filepath.Dir(thisFile))
}

// fixtureYAML - исходник onlineconf-модуля для тестов.
func fixtureYAML() string {
	return filepath.Join(repoRoot(), "build", "local", "config.yaml")
}

// testCDB собирает onlineconf-модуль TREE.cdb из build/local/config.yaml утилитой
// yaml2cdb (go tool из go.mod) во временный каталог теста и возвращает путь к нему.
//
// .cdb-файлы в репозитории не хранятся (см. .gitignore) - модуль генерируется
// тогда, когда он нужен. Генерация занимает миллисекунды, поэтому делается
// на каждый тест, без разделяемого состояния между тестами.
func testCDB(t *testing.T) string {
	t.Helper()

	out := filepath.Join(t.TempDir(), "TREE.cdb")
	buildCDB(t, fixtureYAML(), out)

	return out
}

// buildCDB собирает cdb-модуль из yaml утилитой yaml2cdb.
func buildCDB(t *testing.T, yamlPath, out string) {
	t.Helper()

	cmd := exec.Command("go", "tool", "yaml2cdb", "-in", yamlPath, "-out", out)
	cmd.Dir = repoRoot() // go tool разрешает yaml2cdb через go.mod этого модуля

	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("yaml2cdb: %v\n%s", err, output)
	}
}

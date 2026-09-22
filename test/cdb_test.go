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

// testCDB собирает onlineconf-модуль TREE.cdb из build/local/config.yaml утилитой
// yaml2cdb (go tool из go.mod) во временный каталог теста и возвращает путь к нему.
//
// .cdb-файлы в репозитории не хранятся (см. .gitignore) - модуль генерируется
// тогда, когда он нужен. Генерация занимает миллисекунды, поэтому делается
// на каждый тест, без разделяемого состояния между тестами.
func testCDB(t *testing.T) string {
	t.Helper()

	root := repoRoot()
	out := filepath.Join(t.TempDir(), "TREE.cdb")

	cmd := exec.Command("go", "tool", "yaml2cdb",
		"-in", filepath.Join(root, "build", "local", "config.yaml"),
		"-out", out)
	cmd.Dir = root // go tool разрешает yaml2cdb через go.mod этого модуля

	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("yaml2cdb: %v\n%s", err, output)
	}

	return out
}

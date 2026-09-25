package tptest

import (
	"bytes"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	advslog "github.com/adventures-team/go-adv-slog"
	"github.com/onlineconf/onlineconf-go/v2"
)

// recordingTB реализует advslog.TestingOutput (метод Output из testing.TB),
// собирая вывод лога в память.
type recordingTB struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (r *recordingTB) Output() io.Writer { return r }

func (r *recordingTB) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.buf.Write(p)
}

func (r *recordingTB) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.buf.String()
}

func (r *recordingTB) contains(substr string) bool {
	return strings.Contains(r.String(), substr)
}

// TestPerPackageLogLevels проверяет связку [tp.WithLogLevelConfig] сквозь реальный
// onlineconf-модуль (собирается из build/local/config.yaml): фикстура задаёт оверрайд
// /tp/go/log/github.com/adventures-team/go-tp/test/level = error для import path
// этого тестового пакета, и child lists, без которых advslog не нашёл бы его.
//
// Проверяется и разделение владения уровнями: глобальный уровень задаёт параметр
// log_level (applyConfig), а поддерево - только пер-пакетные оверрайды. Ключа /level
// в поддереве нет, поэтому levelconfig не трогает глобальный advslog.Level.
func TestPerPackageLogLevels(t *testing.T) {
	conf, err := onlineconf.OpenSubtree(testCDB(t), "/")
	if err != nil {
		t.Fatal(err)
	}

	prevDefault := slog.Default()
	defer slog.SetDefault(prevDefault)

	prevLevel := advslog.Level.Level()
	defer advslog.Level.Set(prevLevel)

	// у корня поддерева нет /level - конфиг уровней не должен перехватить
	// глобальный уровень при инициализации (им владеет log_level)
	advslog.Level.Set(slog.LevelWarn)

	rec := &recordingTB{}
	logger := advslog.InitTest(rec, advslog.WithLevelConfig(conf.Subtree("/tp/go/log")))

	if got := advslog.Level.Level(); got != slog.LevelWarn {
		t.Fatalf("global level hijacked by level config: got %v, want %v (the fixture must not contain <root>/level)", got, slog.LevelWarn)
	}

	// глобальный уровень - trace, но для нашего пакета действует оверрайд error
	advslog.Level.Set(advslog.LevelTrace)

	logger.Debug("per-package debug must be suppressed")
	logger.Warn("per-package warn must be suppressed")
	logger.Error("per-package error must pass")

	if rec.contains("per-package debug must be suppressed") {
		t.Error("debug record passed despite the error-level package override")
	}

	if rec.contains("per-package warn must be suppressed") {
		t.Error("warn record passed despite the error-level package override")
	}

	if !rec.contains("per-package error must pass") {
		t.Errorf("error record was dropped; captured output: %q", rec.String())
	}
}

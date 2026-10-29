package main

import (
	"bytes"
	"errors"
	"flag"
	"strings"
	"testing"
)

func TestModuleDefault(t *testing.T) {
	if got := moduleDefault(""); got != DefaultModule {
		t.Errorf("moduleDefault(\"\") = %q, want %q", got, DefaultModule)
	}

	if got := moduleDefault("/etc/onlineconf/TREE.cdb"); got != "/etc/onlineconf/TREE.cdb" {
		t.Errorf("moduleDefault(env) = %q, want the env value", got)
	}
}

func TestParseOptions(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		env     string
		want    options
		wantErr error
		usage   bool // usage напечатан в stderr
	}{
		{name: "env module", args: []string{"-subtree", "/project/queued-mover/"}, env: "/x/TREE.cdb",
			want: options{module: "/x/TREE.cdb", subtree: "/project/queued-mover"}},
		{name: "default module", args: []string{"-subtree", "/p"}, want: options{module: DefaultModule, subtree: "/p"}},
		{name: "flag wins over env", args: []string{"-module", "./local.cdb", "-subtree", "/p"}, env: "/x/TREE.cdb",
			want: options{module: "./local.cdb", subtree: "/p"}},
		{name: "no subtree", args: nil, wantErr: errSubtreeRequired, usage: true},
		{name: "empty subtree", args: []string{"-subtree", ""}, wantErr: errSubtreeRequired, usage: true},
		{name: "relative subtree", args: []string{"-subtree", "p"}, wantErr: errSubtreeRelative, usage: true},
		{name: "empty module", args: []string{"-module", "", "-subtree", "/p"}, wantErr: errModuleEmpty, usage: true},
		{name: "extra args", args: []string{"-subtree", "/p", "extra"}, wantErr: errExtraArgs, usage: true},
		{name: "help", args: []string{"-h"}, wantErr: flag.ErrHelp, usage: true},
		{name: "unknown flag", args: []string{"-bogus"}, wantErr: errUnknownFlag, usage: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stderr bytes.Buffer

			got, err := parseOptions(tt.args, tt.env, &stderr)

			switch tt.wantErr {
			case errUnknownFlag:
				if err == nil || !strings.Contains(err.Error(), "bogus") {
					t.Errorf("parseOptions(%v): err = %v, want an unknown flag error", tt.args, err)
				}
			default:
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("parseOptions(%v): err = %v, want %v", tt.args, err, tt.wantErr)
				}
			}

			if err == nil && got != tt.want {
				t.Errorf("parseOptions(%v) = %+v, want %+v", tt.args, got, tt.want)
			}

			if printed := strings.Contains(stderr.String(), "Usage of queued-mover"); printed != tt.usage {
				t.Errorf("parseOptions(%v): usage printed = %v, want %v; stderr:\n%s", tt.args, printed, tt.usage, stderr.String())
			}
		})
	}
}

// errUnknownFlag - маркер для теста: ошибка пакета flag о неизвестном ключе не экспортирована.
var errUnknownFlag = errors.New("unknown flag")

func TestRunUsageError(t *testing.T) {
	var stderr bytes.Buffer

	if code := run(nil, "", &stderr); code != exitUsage {
		t.Errorf("run() without -subtree: exit code %d, want %d", code, exitUsage)
	}

	if !strings.Contains(stderr.String(), errSubtreeRequired.Error()) {
		t.Errorf("run() without -subtree: stderr %q does not mention %q", stderr.String(), errSubtreeRequired)
	}

	if code := run([]string{"-h"}, "", &stderr); code != exitOK {
		t.Errorf("run(-h): exit code %d, want %d", code, exitOK)
	}
}

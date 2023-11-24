// Package rootpath provides root path of the project.
package rootpath

import (
	"path/filepath"
	"runtime"
)

var (
	_, b, _, _ = runtime.Caller(0)
	// Root contains absolute path of the project
	Root = filepath.Join(filepath.Dir(b), "../..")
)

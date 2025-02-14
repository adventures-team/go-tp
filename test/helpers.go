// Package tptest contains integration tests
package tptest

import (
	"path"

	"gitlab.cebc.tools/go/tp.git/test/rootpath"
)

// ConfigDir returns path to directory with TREE.cdb file.
func ConfigDir() string {
	return path.Join(rootpath.Root, "build", "local")
}

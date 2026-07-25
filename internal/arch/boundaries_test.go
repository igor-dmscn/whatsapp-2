// Package arch_test enforces the architectural boundaries described in
// docs/architecture.md and decided in ADR-0007.
//
// This is a test rather than linter configuration on purpose. ADR-0007 keeps the
// contexts inside one binary, which means nothing physically prevents a
// boundary violation — only this file does. Written as Go, the rules are
// readable, they explain themselves when they fail, and they need no tool
// installed beyond the one already required to build the project.
package arch_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const (
	repoRoot   = "../.."
	modulePath = "comms"
)

// notContexts are directories under internal/ that are not bounded contexts.
// platform is shared plumbing; arch is this test.
var notContexts = map[string]bool{"platform": true, "arch": true}

// TestDomainDependsOnNothing asserts that every context's domain package imports
// only the standard library.
//
// This is the property the whole layering exists to protect. A domain package
// that imports a driver, a framework or a transport has stopped being a model of
// the business and become a description of the plumbing.
func TestDomainDependsOnNothing(t *testing.T) {
	eachGoFile(t, filepath.Join(repoRoot, "internal"), func(path string, imports []string) {
		context, rest, ok := splitContext(path)
		if !ok || !strings.HasPrefix(rest, "domain") {
			return
		}
		for _, imported := range imports {
			if isStdlib(imported) {
				continue
			}
			t.Errorf("%s: domain of context %q imports %q\n"+
				"\tThe domain layer may import only the standard library.\n"+
				"\tDefine a port in the domain and implement it in infrastructure instead.",
				path, context, imported)
		}
	})
}

// TestContextsDoNotImportEachOther asserts that no bounded context imports
// another.
//
// Cross-context collaboration goes one of two ways: an event over Kafka, or a
// port the consuming context defines and the wiring in cmd/ satisfies. Neither
// requires an import, so any import here is a boundary being crossed silently.
// Cross-context references are IDs, never types.
func TestContextsDoNotImportEachOther(t *testing.T) {
	eachGoFile(t, filepath.Join(repoRoot, "internal"), func(path string, imports []string) {
		context, _, ok := splitContext(path)
		if !ok {
			return
		}
		for _, imported := range imports {
			other, ok := importedContext(imported)
			if !ok || other == context {
				continue
			}
			t.Errorf("%s: context %q imports context %q (%q)\n"+
				"\tContexts collaborate through events or through a port defined by\n"+
				"\tthe consumer and wired in cmd/. They do not import each other.",
				path, context, other, imported)
		}
	})
}

// splitContext reports the bounded context a file belongs to, and its path
// within that context. It returns false for files outside a context.
func splitContext(path string) (context, rest string, ok bool) {
	normalised := filepath.ToSlash(filepath.Clean(path))
	index := strings.Index(normalised, "internal/")
	if index < 0 {
		return "", "", false
	}
	segments := strings.Split(normalised[index+len("internal/"):], "/")
	if len(segments) < 2 || notContexts[segments[0]] {
		return "", "", false
	}
	return segments[0], strings.Join(segments[1:], "/"), true
}

// importedContext reports which bounded context an import path refers to, if any.
func importedContext(imported string) (string, bool) {
	prefix := modulePath + "/internal/"
	if !strings.HasPrefix(imported, prefix) {
		return "", false
	}
	segments := strings.Split(strings.TrimPrefix(imported, prefix), "/")
	if notContexts[segments[0]] {
		return "", false
	}
	return segments[0], true
}

// isStdlib reports whether an import path is from the standard library. Module
// paths are domain-qualified, so a first segment containing a dot means external.
func isStdlib(imported string) bool {
	first, _, _ := strings.Cut(imported, "/")
	return !strings.Contains(first, ".")
}

// eachGoFile calls fn with the imports of every non-test Go file under root.
func eachGoFile(t *testing.T, root string, fn func(path string, imports []string)) {
	t.Helper()

	fileSet := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name := entry.Name(); name == "node_modules" || name == "dist" {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		file, err := parser.ParseFile(fileSet, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}

		imports := make([]string, 0, len(file.Imports))
		for _, spec := range file.Imports {
			unquoted, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			imports = append(imports, unquoted)
		}
		fn(path, imports)
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
}

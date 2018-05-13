// +build mage

package main

import (
	"fmt"
	"time"

	"github.com/magefile/mage/sh"
)

// tag returns the git tag for the current branch or "" if none.
func tag() string {
	s, _ := sh.Output("git", "describe", "--tags")
	return s
}

// hash returns the git hash for the current repo or "" if none.
func hash() string {
	hash, _ := sh.Output("git", "rev-parse", "--short", "HEAD")
	return hash
}

func ldflags() (string, error) {
	timestamp := time.Now().Format(time.RFC3339)
	hash := hash()
	tag := tag()
	if tag == "" {
		tag = "dev"
	}
	return fmt.Sprintf(`-X "main.date=%s" -X "main.commit=%s" -X "main.version=%s"`, timestamp, hash, tag), nil
}

// Build create a versioned nsq2kinesis binary.
//
// Something something.
func Build() error {
	ldf, err := ldflags()
	if err != nil {
		return err
	}
	path := "./nsq2kinesis"
	return sh.RunV("go", "build", "-o", path, "-ldflags="+ldf, "github.com/daroot/nsq2kinesis")
}

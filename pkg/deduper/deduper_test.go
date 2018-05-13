package deduper_test

import (
	"testing"

	"github.com/daroot/nsq2kinesis/pkg/deduper"
)

func TestDeduper(t *testing.T) {
	testCases := []struct {
		name     string
		ops      string
		expected bool
	}{
		{"single", "", false},
		{"dupe", "a", true},
		{"double dupe", "aa", true},
		{"multiple dupe", "aaa", true},
		{"trim once", "a#", true},
		{"age out", "a##", false},
		{"post age out", "a##a", true},
		{"trim interleave", "a#a#", true},
		{"interleaved age out", "a#a##", false},
		{"non-target", "b", false},
		{"post non-target", "ab", true},
		{"interleaved non-target", "bab", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			deduper := deduper.New()
			for _, op := range tc.ops {
				switch op {
				case '#':
					deduper.Trim()
				default:
					_ = deduper.Test([]byte(string(op)))
				}
			}

			got := deduper.Test([]byte("a"))
			if got != tc.expected {
				t.Error(tc.name, "got", got, "expecting", tc.expected)
			}
		})
	}
}

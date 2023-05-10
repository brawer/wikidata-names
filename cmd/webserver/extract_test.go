package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestListExtracts(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		"blah-20230518.csv.gz",
		"familynames-20230131.csv.gz",
		"familynames-20230518.csv.gz",
		"givennames-20230131.csv.gz",
		"givennames-20230518.csv.gz",
		"givennames-20231111.csv.gz",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(name), 0644); err != nil {
			t.Fatal(err)
		}
	}

	gotMap, err := ListExtracts(dir)
	if err != nil {
		t.Fatal(err)
	}

	gotVec := make([]string, 0, len(gotMap))
	for k, v := range gotMap {
		base := filepath.Base(v.Path)
		s := fmt.Sprintf("%s:{Path=%s, Etag=%s}", k, base, v.Etag)
		gotVec = append(gotVec, s)
	}
	sort.Strings(gotVec)
	got := strings.Join(gotVec, ", ")

	want := `familynames.csv.gz:{Path=familynames-20230518.csv.gz, Etag=whORxBnAgoUsC45ZMnimRLe6JI0=}, givennames.csv.gz:{Path=givennames-20230518.csv.gz, Etag=YtG04r85Xl65fctqwQ0FFUwdYzA=}`

	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

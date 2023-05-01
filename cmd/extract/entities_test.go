// SPDX-FileCopyrightText: 2023 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestFindEntitiesDump(t *testing.T) {
	dumpsDir := t.TempDir()
	dir := filepath.Join(dumpsDir, "wikidatawiki", "entities")
	if err := os.MkdirAll(filepath.Join(dir, "20250215"), 0755); err != nil {
		t.Error(err)
		return
	}

	dumpPath := filepath.Join(dir, "20250215", "wikidata-20250215-all.json.bz2")
	if f, err := os.Create(dumpPath); err == nil {
		f.Close()
	} else {
		t.Error(err)
		return
	}

	err := os.Symlink(filepath.Join("20250215", "wikidata-20250215-all.json.bz2"),
		filepath.Join(dir, "latest-all.json.bz2"))
	if err != nil {
		t.Error(err)
		return
	}

	wantPath := filepath.Join(dir, "20250215", "wikidata-20250215-all.json.bz2")
	date, path, err := findEntitiesDump(dumpsDir)
	if err != nil {
		t.Error(err)
		return
	}

	if d := date.Format("2006-01-02"); d != "2025-02-15" {
		t.Errorf("want 2025-02-15, got %s", d)
	}

	got, _ := os.Stat(path)
	want, _ := os.Stat(wantPath)
	if !os.SameFile(want, got) {
		t.Errorf("want %q, got %q", wantPath, path)
	}
}

type RoundTripFunc func(req *http.Request) *http.Response

func (f RoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req), nil
}

func NewTestClient(fn RoundTripFunc) *http.Client {
	return &http.Client{
		Transport: RoundTripFunc(fn),
	}
}

func TestQuerySubclasses(t *testing.T) {
	client := NewTestClient(func(req *http.Request) *http.Response {
		var buf bytes.Buffer
		buf.WriteString("class\n")
		buf.WriteString("http://www.wikidata.org/entity/Q123\n")
		buf.WriteString("http://www.wikidata.org/entity/Q987\n")
		return &http.Response{
			StatusCode: 200,
			Header:     make(http.Header),
			Body:       io.NopCloser(&buf),
		}
	})

	gotSet, err := QuerySubclasses(777, client)
	if err != nil {
		t.Error(err)
		return
	}
	gotVec := make([]string, 0, len(gotSet))
	for qid, _ := range gotSet {
		gotVec = append(gotVec, fmt.Sprintf("Q%d", qid))
	}
	sort.Strings(gotVec)

	got := strings.Join(gotVec, ",")
	want := "Q123,Q777,Q987"
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestContainsAny(t *testing.T) {
	a := ClassSet{7: struct{}{}, 9: struct{}{}}
	for _, tc := range []struct {
		ids  []int
		want bool
	}{
		{[]int{}, false},
		{[]int{7}, true},
		{[]int{8}, false},
		{[]int{9}, true},
		{[]int{7, 8, 9}, true},
		{[]int{23, 24, 25}, false},
	} {
		other := make(ClassSet, 0)
		for _, id := range tc.ids {
			other[int64(id)] = struct{}{}
		}
		if got := a.ContainsAny(&other); got != tc.want {
			t.Errorf("got %v, want %v, other=%v", got, tc.want, other)
		}
	}
}

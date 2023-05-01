// SPDX-FileCopyrightText: 2023 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestShouldRun(t *testing.T) {
	dumpDate, _ := time.Parse(time.RFC3339, "2023-04-18T23:22:21Z")
	workdir := t.TempDir()
	if got, err := ShouldRun(dumpDate, workdir); err != nil {
		t.Error(err)
		return
	} else if got != true {
		t.Errorf("expected true, got %v", got)
		return
	}

	f, err := os.Create(filepath.Join(workdir, "familynames-20230418.csv.gz"))
	if err != nil {
		t.Error(err)
		return
	}
	if err := f.Close(); err != nil {
		t.Error(err)
		return
	}

	if got, err := ShouldRun(dumpDate, workdir); err != nil {
		t.Error(err)
		return
	} else if got != true {
		t.Errorf("expected true, got %v", got)
		return
	}

	f, err = os.Create(filepath.Join(workdir, "givennames-20230418.csv.gz"))
	if err != nil {
		t.Error(err)
		return
	}
	if err := f.Close(); err != nil {
		t.Error(err)
		return
	}

	if got, err := ShouldRun(dumpDate, workdir); err != nil {
		t.Error(err)
		return
	} else if got != false {
		t.Errorf("expected false, got %v, workdir=%v", got, workdir)
		return
	}
}

func TestExtractor(t *testing.T) {
	workdir := t.TempDir()
	dumpPath := filepath.Join("testdata", "full", "entities.json.bz2")
	dumpDate, err := time.Parse(time.RFC3339, "2023-04-18T23:22:21Z")
	if err != nil {
		t.Error(err)
		return
	}

	client := NewTestClient(func(req *http.Request) *http.Response {
		const prefix = "SELECT ?subclass WHERE {?subclass wdt:P279* wd:"
		qid := req.URL.Query().Get("query")
		qid = strings.TrimPrefix(qid, prefix)
		qid = strings.TrimSuffix(qid, ". }")
		path := filepath.Join("testdata", "full", fmt.Sprintf("subclasses_of_%s.csv", qid))
		reader, err := os.Open(path)
		if err != nil {
			t.Error(err)
			var buf bytes.Buffer
			buf.WriteString("not found")
			return &http.Response{
				StatusCode: 404,
				Header:     make(http.Header),
				Body:       io.NopCloser(&buf),
			}
		}

		return &http.Response{
			StatusCode: 200,
			Header:     make(http.Header),
			Body:       reader, //io.NopCloser(&buf),
		}
	})

	ex, err := NewExtractor(dumpPath, dumpDate, workdir, client)
	if err != nil {
		t.Error(err)
		return
	}

	if err := ex.Run(); err != nil {
		t.Error(err)
		return
	}

	for _, f := range []string{"givennames", "familynames"} {
		gotPath := filepath.Join(workdir, fmt.Sprintf("%s-20230418.csv.gz", f))
		stream, err := os.Open(gotPath)
		if err != nil {
			t.Error(err)
			return
		}
		defer stream.Close()
		gzStream, err := gzip.NewReader(stream)
		if err != nil {
			t.Error(err)
			return
		}
		gotBytes, err := io.ReadAll(gzStream)
		if err != nil {
			t.Error(err)
			return
		}
		got := string(gotBytes)

		wantPath := filepath.Join("testdata", "full", fmt.Sprintf("want_%s.csv", f))
		wantBytes, err := os.ReadFile(wantPath)
		if err != nil {
			t.Error(err)
			return
		}
		want := string(wantBytes)

		if got != want {
			t.Errorf("%s: got %v, want %v", f, got, want)
		}
	}
}

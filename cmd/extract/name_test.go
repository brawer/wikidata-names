// SPDX-FileCopyrightText: 2023 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"testing"
)

func TestNameToBytes(t *testing.T) {
	want := Name{"Foo", "Q123"}
	got := NameFromBytes(want.ToBytes()).(Name)
	if got.Name != want.Name || got.ID != want.ID {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestNameIsLess(t *testing.T) {
	anna := Name{"Anna", "Q123"}
	bob := Name{"Bob", "Q124"}
	if got := NameIsLess(anna, bob); got != true {
		t.Errorf("got NameIsLess(anna, bob) == %v", got)
	}
	if got := NameIsLess(anna, anna); got != false {
		t.Errorf("got NameIsLess(anna, anna) == %v", got)
	}
}

func TestNameWriter(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewNameWriter(&buf)
	if err != nil {
		t.Error(err)
		return
	}

	if err := w.WriteName(&Name{"Wilde", "Q21050435"}); err != nil {
		t.Error(err)
		return
	}
	if err := w.WriteName(&Name{"Bechdel", "Q4878552"}); err != nil {
		t.Error(err)
		return
	}
	if err := w.WriteName(&Name{"De Beauvoir", "Q104591741"}); err != nil {
		t.Error(err)
		return
	}

	if err := w.Close(); err != nil {
		t.Error(err)
		return
	}

	// Make sure we can close the same writer twice, so that clients can
	// write "defer w.Close()".
	if err := w.Close(); err != nil {
		t.Error(err)
		return
	}

	got := string(buf.Bytes())
	want := ("Name,WikidataID\n" +
		"Bechdel,Q4878552\n" +
		"De Beauvoir,Q104591741\n" +
		"Wilde,Q21050435\n")
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

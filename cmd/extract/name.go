// SPDX-FileCopyrightText: 2023 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"golang.org/x/sync/errgroup"
	"io"
	"sync"

	"github.com/lanrat/extsort"
)

type Name struct {
	Name string
	ID   string
}

func (n Name) ToBytes() []byte {
	var buf bytes.Buffer
	buf.WriteString(n.Name)
	buf.WriteRune(0)
	buf.WriteString(n.ID)
	return buf.Bytes()
}

func NameFromBytes(b []byte) extsort.SortType {
	for i, ch := range b {
		if ch == 0 {
			return Name{Name: string(b[0:i]), ID: string(b[i+1 : len(b)])}
		}
	}
	return Name{}
}

func NameIsLess(a, b extsort.SortType) bool {
	return a.(Name).Name < b.(Name).Name
}

type NameWriter struct {
	mutex    sync.Mutex
	closed   bool
	writer   *csv.Writer
	sortChan chan extsort.SortType
	sortTask *errgroup.Group
}

func NewNameWriter(w io.Writer) (*NameWriter, error) {
	writer := csv.NewWriter(w)
	if err := writer.Write([]string{"Name", "WikidataID"}); err != nil {
		return nil, err
	}

	inChan := make(chan extsort.SortType, 50000)
	sorter, outChan, errChan := extsort.New(inChan, NameFromBytes, NameIsLess, nil)
	task, ctx := errgroup.WithContext(context.Background())
	task.Go(func() error {
		sorter.Sort(ctx)
		return nil
	})
	task.Go(func() error {
		for n := range outChan {
			name := n.(Name)
			if err := writer.Write([]string{name.Name, name.ID}); err != nil {
				return err
			}
		}
		if err := <-errChan; err != nil {
			return err
		}
		return nil
	})
	return &NameWriter{
		writer:   writer,
		sortChan: inChan,
		sortTask: task,
	}, nil
}

func (w *NameWriter) WriteName(n *Name) error {
	w.sortChan <- *n
	return nil
}

func (w *NameWriter) Close() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	if w.closed {
		return nil // already closed before, no work left to do
	}
	w.closed = true
	close(w.sortChan)

	if err := w.sortTask.Wait(); err != nil {
		return err
	}

	w.writer.Flush()

	return nil
}

// SPDX-FileCopyrightText: 2023 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"gitlab.com/tozd/go/errors"
	"gitlab.com/tozd/go/mediawiki"
)

type Extractor struct {
	dumpPath string
	dumpDate time.Time
}

type Output struct {
	path            string
	file            io.WriteCloser
	compressor      *gzip.Writer
	nameWriter      *NameWriter
	wikidataClasses ClassSet
}

func (o *Output) Close() error {
	if err := o.nameWriter.Close(); err != nil {
		return err
	}

	if err := o.compressor.Close(); err != nil {
		return err
	}

	if err := o.file.Close(); err != nil {
		return err
	}

	if err := os.Rename(o.path+".tmp", o.path); err != nil {
		return err
	}

	return nil
}

func ShouldRun(dumpDate time.Time) (bool, error) {
	day := dumpDate.Format("200601012")
	for _, n := range []string{"familynames", "givennames"} {
		path := fmt.Sprintf("%s-%s.csv.gz", n, day)
		_, err := os.Stat(path)
		if err == nil {
			continue
		}
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

func NewExtractor(dumpPath string, dumpDate time.Time) (*Extractor, error) {
	return &Extractor{
		dumpPath: dumpPath,
		dumpDate: dumpDate,
	}, nil
}

func NewOutput(dumpDate time.Time, filename string, wikidataClassID int64, client *http.Client) (*Output, error) {
	day := dumpDate.Format("200601012")
	path := fmt.Sprintf("%s-%s.csv.gz", filename, day)
	file, err := os.Create(path + ".tmp")
	if err != nil {
		return nil, err
	}

	compressor, err := gzip.NewWriterLevel(file, 9)
	if err != nil {
		return nil, err
	}

	nameWriter, err := NewNameWriter(compressor)
	if err != nil {
		return nil, err
	}

	wikidataClasses, err := QuerySubclasses(wikidataClassID, client)
	if err != nil {
		return nil, err
	}

	o := Output{path, file, compressor, nameWriter, wikidataClasses}
	return &o, nil
}

func (ex *Extractor) Run() error {
	client := &http.Client{}

	outputs := make([]*Output, 0)
	for _, s := range []struct {
		filename        string
		wikidataClassID int64
	}{
		{"familynames", 101352},
		{"givennames", 202444},
	} {
		o, err := NewOutput(ex.dumpDate, s.filename, s.wikidataClassID, client)
		if err != nil {
			return err
		}
		outputs = append(outputs, o)
	}

	err := mediawiki.ProcessWikidataDump(
		context.Background(),
		&mediawiki.ProcessDumpConfig{
			Path: ex.dumpPath,
		},
		func(_ context.Context, e mediawiki.Entity) errors.E {
			entityClasses := WikidataClasses(&e)
			for _, o := range outputs {
				if entityClasses.ContainsAny(&o.wikidataClasses) {
					names := make(map[string]struct{}, len(e.Labels))
					for _, langval := range e.Labels {
						names[langval.Value] = struct{}{}
					}
					for name, _ := range names {
						n := Name{Name: name, ID: e.ID}
						if err := o.nameWriter.WriteName(&n); err != nil {
							return errors.WithStack(err)
						}
					}
				}
			}
			return nil
		})
	if err != nil {
		return err
	}

	for _, o := range outputs {
		if err := o.Close(); err != nil {
			return err
		}
	}

	return nil
}

// SPDX-FileCopyrightText: 2023 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	var dumps = flag.String("dumps", "/public/dumps/public", "path to Wikimedia dumps")
	flag.Parse()

	edate, epath, err := findEntitiesDump(*dumps)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	shouldRun, err := ShouldRun(edate)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	if !shouldRun {
		day := edate.Format("2006-01-02")
		fmt.Fprintf(os.Stderr, "already done for Wikidata dump %s\n", day)
		os.Exit(0)
	}

	extractor, err := NewExtractor(epath, edate)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	if err := extractor.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}
}

/*
func build(dump string, outPath string) error {
	fstat, err := os.Stat(outPath)
	if err == nil && !fstat.IsDir() {
		return nil
	}

	tmpPath := outPath + ".tmp"
	outFile, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	gzipWriter, err := gzip.NewWriterLevel(outFile, 0)
	if err != nil {
		return err
	}
	defer gzipWriter.Close()

	nameWriter, err := NewNameWriter(gzipWriter)
	if err != nil {
		return err
	}
	defer nameWriter.Close()

	if err := extractNames(dump, nameWriter); err != nil {
		return err
	}

	if err := nameWriter.Close(); err != nil {
		return err
	}

	if err := gzipWriter.Close(); err != nil {
		return err
	}

	if err := outFile.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, outPath); err != nil {
		return err
	}

	return nil
}
*/

// SPDX-FileCopyrightText: 2023 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
)

func main() {
	var dumps = flag.String("dumps", "/public/dumps/public", "path to Wikimedia dumps")
	var workdir = flag.String("workdir", ".", "path to working directory")
	flag.Parse()

	edate, epath, err := findEntitiesDump(*dumps)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	shouldRun, err := ShouldRun(edate, *workdir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	if !shouldRun {
		day := edate.Format("2006-01-02")
		fmt.Fprintf(os.Stderr, "already done for Wikidata dump %s\n", day)
		os.Exit(0)
	}

	client := &http.Client{}
	extractor, err := NewExtractor(epath, edate, *workdir, client)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	if err := extractor.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}
}

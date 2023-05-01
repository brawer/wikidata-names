// SPDX-FileCopyrightText: 2023 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	//"sync"
	"time"
	//"gitlab.com/tozd/go/errors"
	"gitlab.com/tozd/go/mediawiki"
)

func findEntitiesDump(dumpsPath string) (time.Time, string, error) {
	path := filepath.Join(dumpsPath, "wikidatawiki", "entities", "latest-all.json.bz2")
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return time.Time{}, "", err
	}

	parts := strings.Split(resolved, string(os.PathSeparator))
	date, err := time.Parse("20060102", parts[len(parts)-2])
	if err != nil {
		return time.Time{}, "", err
	}

	// The symlink can change any time on the file system, such as
	// when Wikimedia generates a new dump right between the call
	// to EvalSymlinks() and our client opening the returned path.
	// To avoid this race condition, we need to return the resolved
	// path here, not the symlink.
	return date, resolved, nil
}

type ClassSet map[int64]struct{}

func (self *ClassSet) ContainsAny(other *ClassSet) bool {
	for o, _ := range *other {
		if _, ok := (*self)[o]; ok {
			return true
		}
	}
	return false
}

func WikidataClasses(e *mediawiki.Entity) ClassSet {
	result := make(ClassSet, 3)
	for prop, claims := range e.Claims {
		if prop == "P31" {
			for _, claim := range claims {
				snak := claim.MainSnak
				if snak.SnakType == mediawiki.Value {
					if val, ok := snak.DataValue.Value.(mediawiki.WikiBaseEntityIDValue); ok {
						if qid, err := strconv.ParseInt(val.ID[1:], 10, 64); err == nil {
							result[qid] = struct{}{}
						}
					}
				}
			}
		}
	}
	return result
}

func QuerySubclasses(classID int64, client *http.Client) (ClassSet, error) {
	query := fmt.Sprintf(
		"SELECT ?subclass WHERE {?subclass wdt:P279* wd:Q%d. }",
		classID)
	queryUrl := ("https://query.wikidata.org/sparql?query=" +
		url.QueryEscape(query))

	req, err := http.NewRequest("GET", queryUrl, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Accept", "text/csv")
	req.Header.Add("User-Agent", "WikidataNamesBot/1.0")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	cset := make(ClassSet, 500)
	cset[classID] = struct{}{}
	reader := csv.NewReader(resp.Body)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		const prefix = "http://www.wikidata.org/entity/Q"
		if len(record) == 1 && strings.HasPrefix(record[0], prefix) {
			val, err := strconv.ParseInt(record[0][len(prefix):], 10, 64)
			if err == nil {
				cset[val] = struct{}{}
			}
		}
	}

	return cset, nil
}

func extractNames(path string, w *NameWriter) error {
	if err := w.WriteName(&Name{"Qux", "Q789"}); err != nil {
		return err
	}
	if err := w.WriteName(&Name{"Foo", "Q456"}); err != nil {
		return err
	}
	if err := w.WriteName(&Name{"Bar", "Q123"}); err != nil {
		return err
	}
	return nil
}

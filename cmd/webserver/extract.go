package main

import (
	"crypto/sha1" // not used for security, just for http etag
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"
)

type Extract struct {
	Path         string
	Etag         string
	LastModified time.Time
}

type Extracts map[string]Extract

var filePattern = regexp.MustCompile(`^([a-zA-Z\d_\-]+)-(\d{8})\.csv\.gz$`)

func ListExtracts(path string) (Extracts, error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	datesMap := make(map[string]bool, len(files)/2)
	dirEntries := make(map[string]os.DirEntry, len(files))
	for _, file := range files {
		if m := filePattern.FindStringSubmatch(file.Name()); m != nil {
			datesMap[m[2]] = true
			dirEntries[file.Name()] = file
		}
	}

	dates := make([]string, 0, len(datesMap))
	for d, _ := range datesMap {
		dates = append(dates, d)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(dates)))
	var date string
	dumpNames := []string{"givennames", "familynames"}
	for _, date = range dates {
		allDumpsPresentOnDate := true
		for _, dump := range dumpNames {
			fileName := fmt.Sprintf("%s-%s.csv.gz", dump, date)
			if _, present := dirEntries[fileName]; !present {
				allDumpsPresentOnDate = false
				break
			}
		}
		if allDumpsPresentOnDate {
			extracts := make(Extracts, len(dumpNames))
			for _, dump := range dumpNames {
				fileName := fmt.Sprintf("%s-%s.csv.gz", dump, date)
				info, err := dirEntries[fileName].Info()
				if err != nil {
					return nil, err
				}
				filePath := filepath.Join(path, fileName)
				fileHash, err := hashFile(filePath)
				if err != nil {
					return nil, err
				}
				key := fmt.Sprintf("%s.csv.gz", dump)
				extracts[key] = Extract{
					Path:         filePath,
					Etag:         fileHash,
					LastModified: info.ModTime(),
				}
			}
			return extracts, nil
		}
	}
	return make(Extracts, 0), nil
}

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	hash := sha1.New()
	if _, err := io.Copy(hash, f); err != nil {
		return "", err
	}

	if err := f.Close(); err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(hash.Sum(nil)), nil
}

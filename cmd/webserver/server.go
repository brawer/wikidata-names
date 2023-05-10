package main

import (
	"embed"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

//go:embed homepage.html
var content embed.FS

type Server struct {
	workdir string

	ticker     *time.Ticker
	tickerDone chan bool

	mutex    sync.RWMutex
	extracts Extracts
}

func NewServer(workdir string) (*Server, error) {
	extracts, err := ListExtracts(workdir)
	if err != nil {
		return nil, err
	}

	ticker := time.NewTicker(15 * 60 * time.Second)
	server := Server{
		workdir:    workdir,
		extracts:   extracts,
		ticker:     ticker,
		tickerDone: make(chan bool),
	}

	go func() {
		for {
			select {
			case <-server.tickerDone:
				return
			case t := <-server.ticker.C:
				server.refreshExtracts()
				fmt.Println("Current time: ", t)
			}
		}
	}()

	return &server, nil
}

func (self *Server) HandleHomepage(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != "/" {
		http.NotFound(w, req)
		return
	}

	page, err := content.ReadFile("homepage.html")
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html;charset=utf-8")
	if _, err := w.Write(page); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

func (self *Server) HandleDownload(w http.ResponseWriter, req *http.Request) {
	self.mutex.RLock()
	defer self.mutex.RUnlock()

	filename := strings.TrimPrefix(req.URL.Path, "/downloads/")
	extract, ok := self.extracts[filename]
	if !ok {
		fmt.Println(filename)
		http.NotFound(w, req)
		return
	}

	f, err := os.Open(extract.Path)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("ETag", extract.Etag)
	http.ServeContent(w, req, filename, extract.LastModified, f)
}

func (self *Server) HandleRobotsTxt(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("User-Agent: *\nAllow: /\n"))
}

func (self *Server) refreshExtracts() error {
	extracts, err := ListExtracts(self.workdir)
	if err != nil {
		return err
	}

	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.extracts = extracts

	return nil
}

func (self *Server) Shutdown() error {
	self.ticker.Stop()
	self.tickerDone <- true
	return nil
}

package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
)

func main() {
	port, err := strconv.Atoi(os.Getenv("PORT"))
	if err != nil {
		port = 8080
	}

	server, err := NewServer(".")
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/", server.HandleHomepage)
	http.HandleFunc("/downloads/", server.HandleDownload)
	http.HandleFunc("/robots.txt", server.HandleRobotsTxt)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
		log.Fatal(err)
	}

	if err := server.Shutdown(); err != nil {
		log.Fatal(err)
	}
}

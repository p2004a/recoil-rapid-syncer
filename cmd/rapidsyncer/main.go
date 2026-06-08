// SPDX-FileCopyrightText: 2022,2026 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/beyond-all-reason/recoil-rapid-syncer/pkg/bunny"
	"github.com/beyond-all-reason/recoil-rapid-syncer/pkg/syncer"
)

type Server struct {
	syncer      *syncer.RapidSyncer
	srcRepoRoot string
	httpClient  *http.Client
}

func (s *Server) HandleSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		log.Printf("Got %s, not POST request for URL: %v", r.Method, r.URL)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	var repos []string
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&repos)
	if err != nil {
		log.Printf("Decoding request body: %v", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	synced := make([]int, len(repos))
	var out strings.Builder
	for i, repo := range repos {
		src := syncer.NewHTTPSource(s.srcRepoRoot+repo+"/", s.httpClient)
		synced[i], err = s.syncer.Sync(r.Context(), src, repo)
		if err != nil {
			log.Printf("Failed to sync: %v", err)
			http.Error(w, "Sync Failed", http.StatusInternalServerError)
			return
		}
		msg := fmt.Sprintf("Synced %d archives to %s", synced[i], repo)
		if synced[i] > 0 {
			log.Print(msg)
		}
		out.WriteString(msg)
		out.WriteString("\n")
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, out.String())
}

func main() {
	sourceRapidRepo := os.Getenv("SOURCE_RAPID_REPO")
	if sourceRapidRepo == "" {
		sourceRapidRepo = "https://repos.beyondallreason.dev/"
	}
	bunnyStorageZone := os.Getenv("BUNNY_STORAGE_ZONE")
	if bunnyStorageZone == "" {
		log.Fatalf("Missing required env variable BUNNY_STORAGE_ZONE")
	}
	bunnyAccessKey := os.Getenv("BUNNY_ACCESS_KEY")
	if bunnyAccessKey == "" {
		log.Fatalf("Missing required env variable BUNNY_ACCESS_KEY")
	}

	bunnyClient := bunny.NewClient(bunnyAccessKey)
	zoneClient, err := bunnyClient.NewStorageZoneClient(context.Background(), bunnyStorageZone)
	if err != nil {
		log.Fatalf("Failed to create Bunny storage zone client: %v", err)
	}

	server := &Server{
		syncer:      syncer.NewRapidSyncer(zoneClient),
		srcRepoRoot: sourceRapidRepo,
		httpClient:  syncer.NewHTTPClient(),
	}

	http.HandleFunc("/sync", server.HandleSync)
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("Defaulting to port %s", port)
	}
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}

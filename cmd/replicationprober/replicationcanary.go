// SPDX-FileCopyrightText: 2023 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/beyond-all-reason/recoil-rapid-syncer/pkg/bunny"
)

type Canary struct {
	mu       sync.Mutex
	contents string
}

type StorageReplicationProber struct {
	replicatedFetcher              *ReplicatedFetcher
	bunnyStorageZone               *bunny.StorageZoneClient
	influxdbClient                 *influxdb3.Client
	latestCanary                   Canary
	refreshReplicationCanaryPeriod time.Duration
	checkReplicationStatusPeriod   time.Duration
	canaryFileUrl                  string
	canaryFileName                 string
}

func (s *StorageReplicationProber) startCanaryUpdater(ctx context.Context) {
	ticker := time.NewTicker(s.refreshReplicationCanaryPeriod)
	for {
		c, cancel := context.WithTimeout(ctx, s.refreshReplicationCanaryPeriod/2)
		start := time.Now()
		err := s.updateReplicationCanary(c)
		cancel()
		var points []*influxdb3.Point
		if err != nil {
			log.Printf("WARN: Failed to update replication canary: %v", err)
			points = append(points,
				influxdb3.NewPointWithMeasurement("replication_canary_update_result").
					SetField("total", 1).
					SetField("error", 1).
					SetField("ok", 0).
					SetTimestamp(time.Now()))
		} else {
			latency := time.Since(start)
			points = append(points,
				influxdb3.NewPointWithMeasurement("replication_canary_update_result").
					SetField("total", 1).
					SetField("error", 0).
					SetField("ok", 1).
					SetTimestamp(time.Now()),
				influxdb3.NewPointWithMeasurement("replication_canary_update_latency").
					SetField("latency", latency.Milliseconds()).
					SetTimestamp(time.Now()))
		}

		c, cancel = context.WithTimeout(ctx, s.refreshReplicationCanaryPeriod/3)
		if err := s.influxdbClient.WritePoints(c, points); err != nil {
			log.Printf("WARN: Failed to report replication_canary_update_latency to influxdb: %v", err)
		}
		cancel()

		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
		}
	}
}

func (s *StorageReplicationProber) startReplicationStatusChecker(ctx context.Context) {
	ticker := time.NewTicker(s.checkReplicationStatusPeriod)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
		}

		c, cancel := context.WithTimeout(ctx, s.checkReplicationStatusPeriod/2)
		start := time.Now()
		rs, err := s.fetchReplicationStatus(c)
		cancel()
		var points []*influxdb3.Point
		if err != nil {
			log.Printf("WARN: Failed to fetch replication status: %v", err)
			points = append(points,
				influxdb3.NewPointWithMeasurement("replication_status_check_result").
					SetField("total", 1).
					SetField("error", 1).
					SetField("ok", 0).
					SetTimestamp(time.Now()))
		} else {
			latency := time.Since(start)

			measurementTime := time.Now()

			points = append(points,
				influxdb3.NewPointWithMeasurement("replication_status_check_result").
					SetField("total", 1).
					SetField("error", 0).
					SetField("ok", 1).
					SetTimestamp(measurementTime),
				influxdb3.NewPointWithMeasurement("replication_status_check_latency").
					SetField("latency", latency.Milliseconds()).
					SetTimestamp(measurementTime))

			for _, r := range rs {
				points = append(points,
					influxdb3.NewPointWithMeasurement("replication_status_state").
						SetTag("storage_server", r.StorageServer).
						SetField("replicated", r.Replicated.Unix()).
						SetField("created", r.Created.Unix()).
						SetField("unsynced_for", r.UnsyncedFor.Seconds()).
						SetTimestamp(measurementTime))
			}
		}

		c, cancel = context.WithTimeout(ctx, s.checkReplicationStatusPeriod/3)
		if err := s.influxdbClient.WritePoints(c, points); err != nil {
			log.Printf("WARN: Failed to report replication_status_check_latency to influxdb: %v", err)
		}
		cancel()
	}
}

func (s *StorageReplicationProber) updateReplicationCanary(ctx context.Context) error {
	canary := time.Now().UTC()
	contents := canary.Format(time.RFC3339)

	err := s.bunnyStorageZone.Upload(ctx, s.canaryFileName, strings.NewReader(contents))
	if err != nil {
		return fmt.Errorf("failed to upload replication canary: %w", err)
	}

	s.latestCanary.mu.Lock()
	s.latestCanary.contents = contents
	s.latestCanary.mu.Unlock()
	return nil
}

func fetchFileFromBunnyIP(ctx context.Context, ip, url string) (*ReplicatedFile, error) {
	client := httpClientForAddr(ip+":443", time.Second*20)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set("Cache-Control", "no-cache")
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request to ip %s failed with: %w", ip, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http request failed with code: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("downloading %s failed: %w", url, err)
	}

	storageServerRegion, err := bunny.StorageServerRegionCode(&resp.Header)
	if err != nil {
		return nil, err
	}

	modified, err := http.ParseTime(resp.Header.Get("Last-Modified"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse Last-Modified: %w", err)
	}

	return &ReplicatedFile{
		contents:      body,
		lastModified:  modified,
		etag:          resp.Header.Get("ETag"),
		storageServer: storageServerRegion,
	}, nil
}

type ReplicationStatus struct {
	StorageServer string
	Replicated    time.Time
	Created       time.Time
	UnsyncedFor   time.Duration
}

func (s *StorageReplicationProber) fetchReplicationStatus(ctx context.Context) ([]ReplicationStatus, error) {
	canaryFiles, err := s.replicatedFetcher.FetchReplicatedFile(ctx, s.canaryFileUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch lastes versionsGz: %w", err)
	}

	s.latestCanary.mu.Lock()
	localContents := s.latestCanary.contents
	s.latestCanary.mu.Unlock()
	localCreated, _ := time.Parse(time.RFC3339, localContents)

	rs := make([]ReplicationStatus, len(canaryFiles))
	for i, ver := range canaryFiles {
		contents := string(ver.contents)
		created, err := time.Parse(time.RFC3339, contents)
		if err != nil {
			return nil, fmt.Errorf("failed to parse written time: %w", err)
		}
		var unsyncedFor time.Duration
		if localContents != "" &&
			created.Before(localCreated) {
			unsyncedFor = time.Since(created.Add(s.refreshReplicationCanaryPeriod))
		}
		rs[i] = ReplicationStatus{
			StorageServer: ver.storageServer,
			Replicated:    ver.lastModified,
			Created:       created,
			UnsyncedFor:   unsyncedFor,
		}
	}
	return rs, nil
}

func (s *StorageReplicationProber) HandleReplicationStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		log.Printf("Got %s, not GET request for URL: %v", r.Method, r.URL)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	rs, err := s.fetchReplicationStatus(r.Context())
	if err != nil {
		log.Printf("Failed to fetch replication status: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	var out strings.Builder
	for _, ver := range rs {
		out.WriteString(fmt.Sprintf("%s: \n  created: %s\n  replicated: %s\n  unsynced for: %s\n",
			ver.StorageServer, ver.Created, ver.Replicated, ver.UnsyncedFor))
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, out.String())
}

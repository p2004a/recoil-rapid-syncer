// SPDX-FileCopyrightText: 2023 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/beyond-all-reason/recoil-rapid-syncer/pkg/bunny"
)

type RedirectProber struct {
	bunny                   *bunny.Client
	influxdbClient          *influxdb3.Client
	repo                    string
	versionGzUrl            string
	probePeriod             time.Duration
	edgeServerRefreshPeriod time.Duration
}

var (
	freshVersionsRE = regexp.MustCompile(`^versions_(\d{8}T\d{6}).gz$`)
)

func extractFreshVersion(h *http.Header) (string, error) {
	u, err := url.Parse(h.Get("Location"))
	if err != nil {
		return "", fmt.Errorf("failed to parse location url: %w", err)
	}
	baseFile := path.Base(u.Path)
	m := freshVersionsRE.FindStringSubmatch(baseFile)
	if m == nil {
		return "", fmt.Errorf("failed to parver versions.gz fresh version, got %s", u.Path)
	}
	return m[1], nil
}

type RedirectStatus struct {
	Region  string
	Version string
}

func (p *RedirectProber) fetchVersionGzRedirectLocation(ctx context.Context, ip string) (RedirectStatus, error) {
	client := httpClientForAddr(ip+":443", time.Second*60)
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.versionGzUrl, nil)
	if err != nil {
		return RedirectStatus{}, err
	}
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set("Cache-Control", "no-cache")
	resp, err := client.Do(req)
	if err != nil {
		return RedirectStatus{}, fmt.Errorf("request to ip %s failed with: %w", ip, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMovedPermanently {
		return RedirectStatus{}, fmt.Errorf("http result was not 301, got: %d", resp.StatusCode)
	}

	ver, err := extractFreshVersion(&resp.Header)
	if err != nil {
		return RedirectStatus{}, fmt.Errorf("failed to etract fresh version: %w", err)
	}

	region, err := bunny.ServerRegionCode(&resp.Header)
	if err != nil {
		return RedirectStatus{}, fmt.Errorf("failed to get region code: %w", err)
	}

	return RedirectStatus{Version: ver, Region: region}, nil
}

func (p *RedirectProber) startIPProber(ctx context.Context, ip string) {
	// Add a bunch of jitter so all probers don't kick off at exactly the same time
	<-time.After(time.Duration(rand.Float64() * float64(p.probePeriod)))

	ticker := time.NewTicker(p.probePeriod)
	for {
		c, cancel := context.WithTimeout(ctx, p.probePeriod/2)
		start := time.Now()
		status, err := p.fetchVersionGzRedirectLocation(c, ip)
		cancel()
		var points []*influxdb3.Point
		if err != nil {
			points = append(points,
				influxdb3.NewPointWithMeasurement("versiongz_redirect_check_result").
					SetTag("target", ip).
					SetTag("repo", p.repo).
					SetField("total", 1).
					SetField("error", 1).
					SetField("ok", 0).
					SetTimestamp(time.Now()))
		} else {
			latency := time.Since(start)
			measurementTime := time.Now()
			points = append(points,
				influxdb3.NewPointWithMeasurement("versiongz_redirect_check_result").
					SetTag("target", ip).
					SetTag("repo", p.repo).
					SetField("total", 1).
					SetField("error", 0).
					SetField("ok", 1).
					SetTimestamp(measurementTime),
				influxdb3.NewPointWithMeasurement("versiongz_redirect_check_latency").
					SetTag("target", ip).
					SetTag("repo", p.repo).
					SetField("latency", latency.Milliseconds()).
					SetTimestamp(measurementTime),
				influxdb3.NewPointWithMeasurement("versiongz_redirect_state").
					SetTag("target", ip).
					SetTag("repo", p.repo).
					SetTag("region", status.Region).
					SetField("version", status.Version).
					SetTimestamp(measurementTime))
		}

		c, cancel = context.WithTimeout(ctx, p.probePeriod/3)
		if err := p.influxdbClient.WritePoints(c, points); err != nil {
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

func (p *RedirectProber) startProber(ctx context.Context) {
	ticker := time.NewTicker(p.edgeServerRefreshPeriod)
	ipProbers := make(map[string]context.CancelFunc)
	for {
		c, cancel := context.WithTimeout(ctx, time.Minute*2)
		es, err := p.bunny.EdgeServersIP(c)
		cancel()
		if err != nil {
			log.Printf("ERROR: Failed to get edge servers: %v", err)
			// To make sure we start properly and not have to wait long time to start probing
			if len(ipProbers) == 0 {
				<-time.After(time.Second * 5)
				continue
			}
		} else {
			allNewIPs := make(map[string]struct{})
			for _, ip := range es {
				allNewIPs[ip] = struct{}{}
			}

			for ip := range ipProbers {
				if _, ok := allNewIPs[ip]; !ok {
					ipProbers[ip]()
					delete(ipProbers, ip)
				}
			}

			for ip := range allNewIPs {
				if _, ok := ipProbers[ip]; !ok {
					c, cancel := context.WithCancel(ctx)
					ipProbers[ip] = cancel
					go p.startIPProber(c, ip)
				}
			}
		}

		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
		}
	}
}

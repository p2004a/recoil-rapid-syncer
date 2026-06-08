// SPDX-FileCopyrightText: 2023 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/beyond-all-reason/recoil-rapid-syncer/pkg/bunny"
	"github.com/beyond-all-reason/recoil-rapid-syncer/pkg/sfcache"
	"github.com/caarlos0/env/v11"
)

const UserAgent = "recoil-rapid-syncer/prober 1.0"

const versionsGzRepo = "byar"
const versionsGzFile = "/" + versionsGzRepo + "/versions.gz"

func getExpectedStorageRegions(ctx context.Context, bunnyClient *bunny.Client, storageZone string) ([]string, error) {
	zone, err := bunnyClient.StorageZoneByName(ctx, storageZone)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage zones: %w", err)
	}
	regions := zone.ReplicationRegions[:]
	regions = append(regions, zone.Region)
	return regions, nil
}

type config struct {
	Bunny struct {
		AccessKey   string
		StorageZone string
	} `envPrefix:"BUNNY_"`
	InfluxDb struct {
		Url      string
		Token    string
		Database string
	} `envPrefix:"INFLUXDB_"`
	BaseUrl                        string        `envDefault:"https://repos-cdn.beyondallreason.dev"`
	MaxRegionDistanceKm            float64       `envDefault:"400"`
	StorageEdgeMapCacheDuration    time.Duration `envDefault:"24h"`
	VersionGzCacheDuration         time.Duration `envDefault:"10s"`
	RefreshReplicationCanaryPeriod time.Duration `envDefault:"5m"`
	CheckReplicationStatusPeriod   time.Duration `envDefault:"2m"`
	CheckRedirectStatusPeriod      time.Duration `envDefault:"2m"`
	EnableStorageReplicationProber bool          `envDefault:"true"`
	EnableRedirectStatusProber     bool          `envDefault:"true"`
	Port                           string        `envDefault:"disabled"`
}

func main() {
	cfg := config{}
	if err := env.ParseWithOptions(&cfg, env.Options{
		RequiredIfNoDef:       true,
		UseFieldNameByDefault: true,
	}); err != nil {
		log.Fatalf("%+v\n", err)
	}

	bunnyClient := bunny.NewClient(cfg.Bunny.AccessKey)

	ctx := context.Background()
	bunnyStorageZoneClient, err := bunnyClient.NewStorageZoneClient(ctx, cfg.Bunny.StorageZone)
	if err != nil {
		log.Fatalf("Failed to create Bunny storage zone client: %v", err)
	}

	expectedStorageRegions, err := getExpectedStorageRegions(ctx, bunnyClient, cfg.Bunny.StorageZone)
	if err != nil {
		log.Fatalf("Failed to get expected storage regions: %v", err)
	}

	influxdbClient, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     cfg.InfluxDb.Url,
		Token:    cfg.InfluxDb.Token,
		Database: cfg.InfluxDb.Database,
	})
	if err != nil {
		log.Fatalf("Failed to create InfluxDB client: %v", err)
	}

	rf := &ReplicatedFetcher{
		bunny: bunnyClient,
		storageEdgeMapCache: sfcache.Cache[StorageEdgeMap]{
			Timeout: cfg.StorageEdgeMapCacheDuration,
		},
		maxRegionDistanceKm:    cfg.MaxRegionDistanceKm,
		expectedStorageRegions: expectedStorageRegions,
		probeFileUrl:           cfg.BaseUrl + "/empty.txt",
	}
	http.HandleFunc("/storageedgemap", rf.HandleStorageEdgeMap)

	srp := &StorageReplicationProber{
		replicatedFetcher:              rf,
		bunnyStorageZone:               bunnyStorageZoneClient,
		influxdbClient:                 influxdbClient,
		refreshReplicationCanaryPeriod: cfg.RefreshReplicationCanaryPeriod,
		checkReplicationStatusPeriod:   cfg.CheckRedirectStatusPeriod,
		canaryFileUrl:                  cfg.BaseUrl + "/replication-canary.txt",
		canaryFileName:                 "replication-canary.txt",
	}
	if cfg.EnableStorageReplicationProber {
		go srp.startCanaryUpdater(ctx)
		go srp.startReplicationStatusChecker(ctx)
	}
	http.HandleFunc("/replicationstatus", srp.HandleReplicationStatus)

	redirectProber := &RedirectProber{
		bunny:                   bunnyClient,
		influxdbClient:          influxdbClient,
		repo:                    versionsGzRepo,
		versionGzUrl:            cfg.BaseUrl + versionsGzFile,
		probePeriod:             cfg.CheckRedirectStatusPeriod,
		edgeServerRefreshPeriod: time.Hour * 1,
	}
	if cfg.EnableRedirectStatusProber {
		go redirectProber.startProber(ctx)
	}

	versionGzFetcher := &VersionGzFetcher{
		replicatedFetcher: rf,
		versionGzUrl:      cfg.BaseUrl + versionsGzFile,
		versionsGzCache: sfcache.Cache[[]*ReplicatedFile]{
			Timeout: cfg.VersionGzCacheDuration,
		},
	}
	http.HandleFunc("/replicationstatus_versiongz", versionGzFetcher.HandleReplicationStatusVersionsGz)

	if cfg.Port == "disabled" {
		select {}
	} else {
		if err := http.ListenAndServe(":"+cfg.Port, nil); err != nil {
			log.Fatal(err)
		}
	}
}

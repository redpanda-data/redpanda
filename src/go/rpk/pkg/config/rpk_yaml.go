// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

type (
	// RpkYaml contains the configuration for ~/.config/rpk/config.yml, the
	// next generation of rpk's configuration file.
	RpkYaml struct {
		Version        int            `yaml:"version"`
		CurrentContext string         `yaml:"current_context,omitempty"`
		Contexts       []RpkContext   `yaml:"contexts,omitempty"`
		CloudAuths     []RpkCloudAuth `yaml:"cloud_auth,omitempty"`
		Tuners         *RpkTuners     `yaml:"tuners,omitempty"`
	}

	RpkContext struct {
		Name         string           `yaml:"name,omitempty"`
		CloudCluster *RpkCloudCluster `yaml:"cloud_cluster,omitempty"`
		KafkaAPI     *RpkKafkaAPI     `yaml:"kafka_api,omitempty"`
		AdminAPI     *RpkAdminAPI     `yaml:"admin_api,omitempty"`
	}

	RpkCloudCluster struct {
		Namespace string `yaml:"namespace"`
		Cluster   string `yaml:"cluster"`
		Auth      string `yaml:"auth"`
	}

	RpkCloudAuth struct {
		Token          string `yaml:"token,omitempty"`
		RefreshToken   string `yaml:"refresh_token,omitempty"`
		ClientID       string `yaml:"client_id,omitempty"`
		ClientSecret   string `yaml:"client_secret,omitempty"`
		OrganizationID string `yaml:"organization,omitempty"`
		Name           string `yaml:"name,omitempty"`
	}

	RpkTuners struct {
		Network              bool   `yaml:"network,omitempty"`
		DiskScheduler        bool   `yaml:"disk_scheduler,omitempty"`
		Nomerges             bool   `yaml:"disk_nomerges,omitempty"`
		DiskWriteCache       bool   `yaml:"disk_write_cache,omitempty"`
		DiskIrq              bool   `yaml:"disk_irq,omitempty"`
		Fstrim               bool   `yaml:"fstrim,omitempty"`
		CPU                  bool   `yaml:"cpu,omitempty"`
		AioEvents            bool   `yaml:"aio_events,omitempty"`
		Clocksource          bool   `yaml:"clocksource,omitempty"`
		Swappiness           bool   `yaml:"swappiness,omitempty"`
		TransparentHugePages bool   `yaml:"transparent_hugepages,omitempty"`
		Coredump             bool   `yaml:"coredump,omitempty"`
		CoredumpDir          string `yaml:"coredump_dir,omitempty"`
		BallastFile          bool   `yaml:"ballast_file,omitempty"`
		BallastFilePath      string `yaml:"ballast_file_path,omitempty"`
		BallastFileSize      string `yaml:"ballast_file_size,omitempty"`
		WellKnownIo          string `yaml:"well_known_io,omitempty"`
	}
)

func (t *RpkTuners) asNodeTuners() RpkNodeTuners {
	if t == nil {
		return RpkNodeTuners{}
	}
	return RpkNodeTuners{
		TuneNetwork:              t.Network,
		TuneDiskScheduler:        t.DiskScheduler,
		TuneNomerges:             t.Nomerges,
		TuneDiskWriteCache:       t.DiskWriteCache,
		TuneDiskIrq:              t.DiskIrq,
		TuneFstrim:               t.Fstrim,
		TuneCPU:                  t.CPU,
		TuneAioEvents:            t.AioEvents,
		TuneClocksource:          t.Clocksource,
		TuneSwappiness:           t.Swappiness,
		TuneTransparentHugePages: t.TransparentHugePages,
		TuneCoredump:             t.Coredump,
		CoredumpDir:              t.CoredumpDir,
		TuneBallastFile:          t.BallastFile,
		BallastFilePath:          t.BallastFilePath,
		BallastFileSize:          t.BallastFileSize,
		WellKnownIo:              t.WellKnownIo,
	}
}

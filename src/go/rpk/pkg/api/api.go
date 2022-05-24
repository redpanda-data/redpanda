// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/version"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

const defaultUrl = "https://m.rp.vectorized.io"

type MetricsPayload struct {
	FreeMemoryMB  float64 `json:"freeMemoryMB"`
	FreeSpaceMB   float64 `json:"freeSpaceMB"`
	CpuPercentage float64 `json:"cpuPercentage"`
	Partitions    *int    `json:"partitions"`
	Topics        *int    `json:"topics"`
}

type EnvironmentPayload struct {
	Checks   []CheckPayload `json:"checks"`
	Tuners   []TunerPayload `json:"tuners"`
	ErrorMsg string         `json:"errorMsg"`
}

type CheckPayload struct {
	Name     string `json:"name"`
	ErrorMsg string `json:"errorMsg"`
	Current  string `json:"current"`
	Required string `json:"required"`
}

type TunerPayload struct {
	Name      string `json:"name"`
	ErrorMsg  string `json:"errorMsg"`
	Enabled   bool   `json:"enabled"`
	Supported bool   `json:"supported"`
}

type metricsBody struct {
	MetricsPayload
	SentAt       time.Time `json:"sentAt"`
	NodeUuid     string    `json:"nodeUuid"`
	Organization string    `json:"organization"`
	ClusterId    string    `json:"clusterId"`
	NodeId       int       `json:"nodeId"`
}

type environmentBody struct {
	Payload      EnvironmentPayload `json:"payload"`
	SentAt       time.Time          `json:"sentAt"`
	NodeUuid     string             `json:"nodeUuid"`
	Organization string             `json:"organization"`
	ClusterId    string             `json:"clusterId"`
	NodeId       int                `json:"nodeId"`
	CloudVendor  string             `json:"cloudVendor"`
	VMType       string             `json:"vmType"`
	OSInfo       string             `json:"osInfo"`
	CPUModel     string             `json:"cpuModel"`
	CPUCores     int                `json:"cpuCores"`
	RPVersion    string             `json:"rpVersion"`
	Environment  string             `json:"environment"`
}

func SendMetrics(p MetricsPayload, conf config.Config) error {
	b := metricsBody{
		p,
		time.Now(),
		conf.NodeUuid,
		conf.Organization,
		conf.ClusterId,
		conf.Redpanda.Id,
	}
	return sendMetricsToUrl(b, defaultUrl, conf)
}

func SendEnvironment(
	fs afero.Fs,
	env EnvironmentPayload,
	conf config.Config,
	skipCloudCheck bool,
) error {
	cloudVendor := "N/A"
	vmType := "N/A"
	if !skipCloudCheck {
		v, err := cloud.AvailableVendor()
		if err != nil {
			log.Debug(err)
		} else {
			cloudVendor = v.Name()
			vt, err := v.VmType()
			if err != nil {
				log.Debug("Error retrieving instance type: ", err)
			} else {
				vmType = vt
			}
		}
	}

	osInfo, err := system.UnameAndDistro(2000 * time.Millisecond)
	if err != nil {
		log.Debug("Error querying OS info: ", err)
		osInfo = "N/A"
	} else {
		osInfo = stripCtlFromUTF8(osInfo)
	}
	cpuModel := "N/A"
	cpuCores := 0
	cpuInfo, err := system.CpuInfo(fs)
	if err != nil {
		log.Debug("Error querying CPU info: ", err)
	} else if len(cpuInfo) > 0 {
		cpuModel = cpuInfo[0].ModelName
		cpuCores = int(cpuInfo[0].Cores) * len(cpuInfo)
	}

	b := environmentBody{
		Payload:      env,
		SentAt:       time.Now(),
		NodeUuid:     conf.NodeUuid,
		Organization: conf.Organization,
		ClusterId:    conf.ClusterId,
		NodeId:       conf.Redpanda.Id,
		CloudVendor:  cloudVendor,
		VMType:       vmType,
		OSInfo:       osInfo,
		CPUModel:     cpuModel,
		CPUCores:     cpuCores,
		RPVersion:    version.Pretty(),
	}
	return sendEnvironmentToUrl(
		b,
		fmt.Sprintf("%s%s", defaultUrl, "/env"),
		conf,
	)
}

func stripCtlFromUTF8(str string) string {
	return strings.Map(func(r rune) rune {
		if r >= 32 && r != 127 {
			return r
		}
		return -1
	}, str)
}

func sendMetricsToUrl(b metricsBody, url string, conf config.Config) error {
	bs, err := json.Marshal(b)
	if err != nil {
		return err
	}
	return sendRequest(bs, http.MethodPost, url, conf)
}

func sendEnvironmentToUrl(
	body environmentBody, url string, conf config.Config,
) error {
	body.Environment = os.Getenv("REDPANDA_ENVIRONMENT")
	bs, err := json.Marshal(body)
	if err != nil {
		return err
	}
	return sendRequest(bs, http.MethodPost, url, conf)
}

func sendRequest(body []byte, method, url string, conf config.Config) error {
	if !conf.Rpk.EnableUsageStats {
		log.Debug("Sending usage stats is disabled.")
		return nil
	}
	req, err := http.NewRequest(
		http.MethodPost,
		url,
		bytes.NewBuffer(body),
	)
	if err != nil {
		return err
	}
	log.Debugf("%s '%s' body='%s'", method, url, body)
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return fmt.Errorf("metrics request failed. Status: %d", res.StatusCode)
	}
	return nil
}

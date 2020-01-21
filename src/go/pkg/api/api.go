package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"vectorized/pkg/config"
)

const defaultUrl = "https://metrics.redpanda.vectorized.io"

type MetricsPayload struct {
	FreeMemory    uint64 `json:"freeMemory"`
	FreeSpace     uint64 `json:"freeSpace"`
	CpuPercentage uint64 `json:"cpuPercentage"`
}

type metricsBody struct {
	MetricsPayload
	NodeUuid     string `json:"nodeUuid"`
	Organization string `json:"organization"`
	ClusterId    string `json:"clusterId"`
	NodeId       int    `json:"nodeId"`
}

func SendMetrics(p MetricsPayload, conf config.Config) error {
	b := metricsBody{
		p,
		conf.NodeUuid,
		conf.Organization,
		conf.ClusterId,
		conf.Redpanda.Id,
	}
	return SendMetricsToUrl(b, defaultUrl)
}

func SendMetricsToUrl(b metricsBody, url string) error {
	bs, err := json.Marshal(b)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(bs))
	if err != nil {
		return err
	}
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

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
	NodeUuid     string `json:"nodeUuid"`
	Organization string `json:"organization"`
	ClusterId    string `json:"clusterId"`
	NodeId       int    `json:"nodeId"`
}

type environmentBody struct {
	EnvironmentPayload
	Config config.Config `json:"config"`
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

func SendEnvironment(env EnvironmentPayload, conf config.Config) error {
	return SendEnvironmentToUrl(
		env,
		conf,
		fmt.Sprintf("%s%s", defaultUrl, "/env"),
	)
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

func SendEnvironmentToUrl(
	env EnvironmentPayload, conf config.Config, url string,
) error {
	body := environmentBody{env, conf}
	bs, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(
		http.MethodPost,
		url,
		bytes.NewBuffer(bs),
	)
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

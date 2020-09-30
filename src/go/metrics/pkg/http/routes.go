package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"metrics/pkg/storage"
	"net"
	"net/http"
	"path"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

type Environment struct {
	ReceivedAt   time.Time
	SentAt       time.Time              `json:"sentAt,omitempty"`
	Organization string                 `json:"organization,omitempty"`
	ClusterId    string                 `json:"clusterId,omitempty"`
	NodeId       int                    `json:"nodeId,omitempty"`
	NodeUuid     string                 `json:"nodeUuid,omitempty"`
	CloudVendor  string                 `json:"cloudVendor,omitempty"`
	VMType       string                 `json:"vmType,omitempty"`
	OSInfo       string                 `json:"osInfo,omitempty"`
	CPUModel     string                 `json:"cpuModel,omitempty"`
	CPUCores     int                    `json:"cpuCores,omitempty"`
	RPVersion    string                 `json:"rpVersion,omitempty"`
	Payload      map[string]interface{} `json:"payload"`
	Config       map[string]interface{} `json:"config"`
	Country      string
	Region       string
	City         string
	IP           string
	Hostname     string
}

func (e *Environment) toStorageEnv() (*storage.Environment, error) {
	payloadJSON, err := json.Marshal(e.Payload)
	if err != nil {
		return nil, err
	}
	configJSON, err := json.Marshal(e.Config)
	if err != nil {
		return nil, err
	}
	return &storage.Environment{
		ReceivedAt:   e.ReceivedAt,
		SentAt:       e.SentAt,
		Organization: e.Organization,
		ClusterId:    e.ClusterId,
		NodeId:       e.NodeId,
		NodeUuid:     e.NodeUuid,
		CloudVendor:  e.CloudVendor,
		VMType:       e.VMType,
		OSInfo:       e.OSInfo,
		CPUModel:     e.CPUModel,
		CPUCores:     e.CPUCores,
		RPVersion:    e.RPVersion,
		Payload:      string(payloadJSON),
		Config:       string(configJSON),
		Country:      e.Country,
		Region:       e.Region,
		City:         e.City,
		IP:           e.IP,
		Hostname:     e.Hostname,
	}, nil
}

type Server struct {
	Port    uint
	Metrics *MetricsHandler
	Env     *EnvHandler
}

type MetricsHandler struct {
	Repo storage.Repository
}
type EnvHandler struct {
	Repo storage.Repository
}

func (s *Server) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	var head string
	head, req.URL.Path = pathHead(req.URL.Path)
	switch head {
	case "":
		s.Metrics.ServeHTTP(res, req)
		return
	case "env":
		s.Env.ServeHTTP(res, req)
		return
	}
	http.Error(res, "Not Found", http.StatusNotFound)
}

func (h *MetricsHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		msg := "Method not allowed"
		log.Error(msg)
		http.Error(res, msg, http.StatusMethodNotAllowed)
		return
	}
	bs, err := ioutil.ReadAll(req.Body)
	if err != nil {
		msg := "Corrupt body"
		log.Error(msg)
		http.Error(res, msg, http.StatusBadRequest)
		return
	}
	log.Infof("Processing '%s'", string(bs))
	metrics := &storage.Metrics{}
	err = json.Unmarshal(bs, metrics)
	if err != nil {
		log.Error(err.Error())
		http.Error(res, err.Error(), http.StatusBadRequest)
		return
	}
	metrics.ReceivedAt = time.Now()
	err = h.Repo.SaveMetrics(*metrics)
	if err != nil {
		log.Error(err.Error())
		http.Error(res, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Infof("Saved '%v+'", metrics)
	res.WriteHeader(http.StatusOK)
}

func (h *EnvHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(res, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	bs, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(res, "Corrupt body", http.StatusBadRequest)
		return
	}
	log.Infof("Processing '%s'", string(bs))
	env := &Environment{}
	err = json.Unmarshal(bs, env)
	if err != nil {
		log.Error(err.Error())
		http.Error(res, err.Error(), http.StatusBadRequest)
		return
	}
	env.ReceivedAt = time.Now()
	env.IP = req.Header.Get("X-Appengine-User-IP")
	env.Country = req.Header.Get("X-Appengine-Country")
	env.Region = req.Header.Get("X-Appengine-Region")
	env.Hostname = resolveIP(env.IP)
	storageEnv, err := env.toStorageEnv()
	if err != nil {
		http.Error(res, "Corrupt body", http.StatusBadRequest)
		return
	}
	err = h.Repo.SaveEnvironment(*storageEnv)
	if err != nil {
		log.Error(err.Error())
		http.Error(res, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Infof("Saved '%s'", string(bs))
	res.WriteHeader(http.StatusOK)
}

func Serve(s *Server) error {
	return http.ListenAndServe(fmt.Sprintf(":%d", s.Port), s)
}

func pathHead(p string) (head, tail string) {
	p = path.Clean("/" + p)
	i := strings.Index(p[1:], "/") + 1
	if i <= 0 {
		return p[1:], "/"
	}
	return p[1:i], p[i:]
}

func resolveIP(ip string) (hostname string) {
	r := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			d := net.Dialer{
				Timeout: time.Millisecond * time.Duration(10000),
			}
			return d.DialContext(ctx, network, "8.8.8.8:53")
		},
	}

	host, err := r.LookupAddr(context.Background(), ip)

	if err != nil {
		log.Error(err.Error())
		return "N/A"
	}

	return host[0]
}

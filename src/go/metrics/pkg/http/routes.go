package http

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"metrics/pkg/storage"
	"net/http"
	"path"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

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
	log.Info(string(bs))
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

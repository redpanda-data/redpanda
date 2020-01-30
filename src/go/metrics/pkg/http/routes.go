package http

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"strings"

	log "github.com/sirupsen/logrus"
)

type Server struct {
	Port    uint
	Metrics *MetricsHandler
	Env     *EnvHandler
}

type MetricsHandler struct{}
type EnvHandler struct{}

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
